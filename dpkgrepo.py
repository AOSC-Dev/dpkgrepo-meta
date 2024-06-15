#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2017 AOSC-Dev <aosc@members.fsf.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA 02110-1301, USA.

import os
import lzma
import psycopg2
import hashlib
import logging
import calendar
import argparse
import collections
import urllib.parse
from email.utils import parsedate

try:
    import httpx as requests
except ImportError:
    import requests

import deb822

logging.basicConfig(
    format="%(asctime)s %(levelname).1s %(message)s", level=logging.INFO
)

# base (list all arch)
#  - amd64
#    - stable
#    - testing
#    - explosive
#  - arm64
#  - ...
#  - noarch
# overlay (list avail arch)
#  - bsp-sunxi
#    - arm64
#      - stable
#      - testing
#      - explosive
#    - armel
#  - opt-avx2
#    - amd64

Repo = collections.namedtuple(
    "Repo",
    (
        "name",  # primary key
        "realname",  # overlay-arch, group key
        "source_tree",  # git source
        "category",  # base, bsp, overlay
        "testing",  # 0-2, testing level
        "suite",  # deb source suite/distribution, git branch
        "component",  # deb source component
        "architecture",  # deb source architecture
    ),
)

ARCHS = ("amd64", "arm64", "loongson3", "ppc64el", "noarch")
BRANCHES = ("stable",)
OVERLAYS = (
    # dpkg_repos.category, component, source, arch
    ("base", "main", None, ARCHS),
)
REPOS = collections.OrderedDict((k, []) for k in BRANCHES)
for category, component, source, archs in OVERLAYS:
    for arch in archs:
        for testlvl, branch in enumerate(BRANCHES):
            if category == "base":
                realname = arch
            else:
                realname = "%s-%s" % (component, arch)
            REPOS[branch].append(
                Repo(
                    realname + "/" + branch,
                    realname,
                    source,
                    category,
                    testlvl,
                    branch,
                    component,
                    arch,
                )
            )


def _url_slash(url):
    if url[-1] == "/":
        return url
    return url + "/"


def init_db(db, with_stats=True):
    cur = db.cursor()
    cur.execute(
        """
        -- prepend length to input e.g. 10 -> 110, 100 -> 2100
        CREATE OR REPLACE FUNCTION public._comparable_digit (digit text)
        RETURNS text AS $$
        -- prepnd length
        SELECT chr(47 + length(v)) || v
        -- trim leading zeros, handle zero string
        FROM (SELECT CASE WHEN v='' THEN '0' ELSE v END v
            FROM (SELECT trim(leading '0' from digit) v) q1
        ) q2
        $$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

        -- convert version into comparable string
        -- e.g. 1.2.0-1 becomes five rows:
        -- "{,1,.2.0-1}"
        -- "{.,2,.0-1}"
        -- "{.,0,-1}"
        -- "{-,1,}"
        -- "{,,}"
        -- after translation:
        -- 101
        -- h102
        -- h100
        -- g101
        -- 1
        -- result is concatenated: 101h102h100g1011
        -- note: sql array indices are 1-based
        CREATE OR REPLACE FUNCTION public.comparable_ver (ver text)
        RETURNS text AS $$
        -- split valid version parts
        WITH RECURSIVE q1 AS (
            SELECT regexp_match($1, '^([^0-9]*)([0-9]*)(.*)$') v
            UNION ALL
            SELECT regexp_match(v[3], '^([^0-9]*)([0-9]*)(.*)$') FROM q1
            WHERE v[3]!='' OR v[2]!=''
        )
        -- for each path, prepend its length and map
        SELECT string_agg(translate(v[1] || '|',
            '~|ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz+-.',
            '0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefgh') ||
            (CASE WHEN v[2]='' THEN '' ELSE _comparable_digit(v[2]) END), '')
        FROM q1
        $$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE COST 200;

        -- convert dpkg version (consider epochs) to comparable format
        CREATE OR REPLACE FUNCTION public.comparable_dpkgver (ver text)
        RETURNS text AS $$
        SELECT ecmp || '!' || (CASE WHEN array_length(spl, 1)=1
            THEN comparable_ver(spl[1]) || '!1'
            ELSE comparable_ver(array_to_string(spl[1:array_length(spl, 1)-1], '-'))
            || '!' || comparable_ver(spl[array_length(spl, 1)]) END)
        FROM (
            -- ecmp: epoch converted to comparable digit format
            -- spl: version without epoch
            SELECT (CASE WHEN epos=0 THEN '00'
            ELSE _comparable_digit(substr(v, 1, epos-1)) END) ecmp, string_to_array(
            CASE WHEN epos=0 THEN v ELSE substr(v, epos+1) END, '-') spl
            FROM (SELECT position(':' in ver) epos, ver v) q1
        ) q1
        $$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE COST 200;

        -- compare dpkg version using custom comparator
        CREATE OR REPLACE FUNCTION public.compare_dpkgver (a text, op text, b text)
        RETURNS bool AS $$
        SELECT CASE WHEN op IS NULL THEN TRUE
            WHEN op='<<' THEN a < b
            WHEN op='<=' THEN a <= b
            WHEN op='=' THEN a = b
            WHEN op='>=' THEN a >= b
            WHEN op='>>' THEN a > b ELSE NULL END
        $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE COST 200;

        -- return bigger dpkg version
        CREATE OR REPLACE FUNCTION public._max_dpkgver (a text, b text)
        RETURNS text AS $$
        SELECT CASE WHEN comparable_dpkgver(a) > comparable_dpkgver(b) THEN a
            ELSE b END
        $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE COST 200;

        -- create custom aggregation for max version
        CREATE OR REPLACE AGGREGATE max_dpkgver (text)
        (
            sfunc = _max_dpkgver,
            stype = text,
            initcond = ''
        );
                """
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS dpkg_repos ("
        "name TEXT PRIMARY KEY,"  # key: bsp-sunxi-armel/testing
        "realname TEXT,"  # group key: amd64, bsp-sunxi-armel
        "source_tree TEXT,"  # abbs tree
        "category TEXT,"  # base, bsp, overlay
        "testing INTEGER,"  # 0, 1, 2
        "suite TEXT,"  # stable, testing, explosive
        "component TEXT,"  # main, bsp-sunxi, opt-avx2
        "architecture TEXT,"  # amd64, all
        "origin TEXT,"
        "label TEXT,"
        "codename TEXT,"
        "date INTEGER,"
        "valid_until INTEGER,"
        "description TEXT"
        ")"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS dpkg_packages ("
        "package TEXT,"
        "version TEXT,"
        "architecture TEXT,"
        "repo TEXT,"
        "maintainer TEXT,"
        "installed_size BIGINT,"
        "filename TEXT,"
        "size BIGINT,"
        "sha256 TEXT,"
        "_vercomp TEXT GENERATED ALWAYS AS (comparable_dpkgver(version)) STORED,"
        # we have Section and Description in packages table
        "PRIMARY KEY (package, version, architecture, repo)"
        # 'FOREIGN KEY(package) REFERENCES packages(name)'
        ")"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS dpkg_package_dependencies ("
        "package TEXT,"
        "version TEXT,"
        "architecture TEXT,"
        "repo TEXT,"
        "relationship TEXT,"
        "value TEXT,"
        "PRIMARY KEY (package, version, architecture, repo, relationship)"
        # 'FOREIGN KEY(package) REFERENCES dpkg_packages(package)'
        ")"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS dpkg_package_duplicate ("
        "package TEXT,"
        "version TEXT,"
        "architecture TEXT,"
        "repo TEXT,"
        "maintainer TEXT,"
        "installed_size BIGINT,"
        "filename TEXT,"
        "size BIGINT,"
        "sha256 TEXT,"
        "PRIMARY KEY (repo, filename)"
        ")"
    )
    if with_stats:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS dpkg_repo_stats ("
            "repo TEXT PRIMARY KEY,"
            "packagecnt INTEGER,"
            "ghostcnt INTEGER,"
            "laggingcnt INTEGER,"
            "missingcnt INTEGER,"
            "oldcnt INTEGER,"
            "FOREIGN KEY(repo) REFERENCES dpkg_repos(name)"
            ")"
        )
        cur.execute(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS v_dpkg_packages_new AS "
            "SELECT DISTINCT ON (package, architecture, repo) "
            "  dp.package package, "
            "  dp.version dpkg_version, "
            "  dp.repo repo, dr.realname reponame, "
            "  dr.architecture architecture, "
            "  dr.suite branch, dp._vercomp _vercomp "
            "FROM dpkg_packages dp "
            "LEFT JOIN dpkg_repos dr ON dp.repo = dr.name "
            "ORDER BY package, architecture, repo ASC, _vercomp DESC"
        )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dpkg_repos" " ON dpkg_repos (realname)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_dpkg_packages"
        " ON dpkg_packages (package, repo)"
    )
    db.commit()
    cur.close()


def remove_clearsign(blob):
    clearsign_header = b"-----BEGIN PGP SIGNED MESSAGE-----"
    pgpsign_header = b"-----BEGIN PGP SIGNATURE-----"
    if not blob.startswith(clearsign_header):
        return blob
    lines = []
    content = False
    for k, ln in enumerate(blob.splitlines(True)):
        if not ln.rstrip():
            content = True
        elif content:
            if ln.rstrip() == pgpsign_header:
                break
            elif ln.startswith(b"- "):
                lines.append(ln[2:])
            else:
                lines.append(ln)
    return b"".join(lines)


def download_catalog(url, local=False, timeout=120, ignore404=False):
    if local:
        urlsp = urllib.parse.urlsplit(url)
        filename = (urlsp.netloc + urlsp.path).replace("/", "_")
        try:
            with open("/var/lib/apt/lists/" + filename, "rb") as f:
                content = f.read()
            return content
        except FileNotFoundError:
            pass
    req = requests.get(url, timeout=120)
    if ignore404 and req.status_code == 404:
        return None
    req.raise_for_status()
    return req.content


def suite_update(db, mirror, suite, repos=None, local=False, force=False):
    """
    Fetch and parse InRelease file. Update relavant metadata.
    suite: branch
    repos: list of Repos
    """
    url = urllib.parse.urljoin(mirror, "/".join(("dists", suite, "InRelease")))
    content = download_catalog(url, local)
    cur = db.cursor()
    if content is None:
        logging.error("dpkg suite %s not found" % suite)
        if not repos:
            return {}
        for repo in repos:
            cur.execute(
                "UPDATE dpkg_repos SET origin = null, label = null, "
                "codename = null, date = null, valid_until = null, description = null "
                "WHERE name = %s",
                (repo.name,),
            )
            cur.execute(
                "DELETE FROM dpkg_package_dependencies WHERE repo = %s", (repo.name,)
            )
            cur.execute(
                "DELETE FROM dpkg_package_duplicate WHERE repo = %s", (repo.name,)
            )
            cur.execute("DELETE FROM dpkg_packages WHERE repo = %s", (repo.name,))
        db.commit()
        return {}
    releasetxt = remove_clearsign(content).decode("utf-8")
    rel = deb822.Release(releasetxt)
    pkgrepos = []
    for item in rel["SHA256"]:
        path = item["name"].split("/")
        if path[-1] == "Packages.xz":
            arch = path[1].split("-")[-1]
            if arch == "all":
                arch = "noarch"
            component = path[0]
            if component.startswith("bsp-"):
                continue
            if component == "main":
                realname = arch
            else:
                realname = "%s-%s" % (component, arch)
            name = "%s/%s" % (realname, suite)
            repo = Repo(name, realname, None, "base", 0, suite, component, arch)
            pkgrepos.append((repo, item["name"], int(item["size"]), item["sha256"]))
    rel_date = calendar.timegm(parsedate(rel["Date"])) if "Date" in rel else None
    rel_valid = None
    if "Valid-Until" in rel:
        rel_valid = calendar.timegm(parsedate(rel["Valid-Until"]))
    result_repos = {}
    repo_dict = {}
    if repos:
        repo_dict = {(r.component, r.architecture): r for r in repos}
    for pkgrepo, filename, size, sha256 in pkgrepos:
        try:
            repo = repo_dict.pop((pkgrepo.component, pkgrepo.architecture))
        except KeyError:
            repo = pkgrepo
        cur.execute("SELECT date FROM dpkg_repos WHERE name = %s", (repo.name,))
        res = cur.fetchone()
        if res and rel_date and not force:
            if res[0] and res[0] >= rel_date:
                continue
        pkgpath = "/".join(("dists", suite, filename))
        result_repos[repo.component, repo.architecture] = (repo, pkgpath, size, sha256)
        cur.execute(
            "INSERT INTO dpkg_repos VALUES "
            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (name) "
            "DO UPDATE SET "
            "realname = excluded.realname, "
            "source_tree = excluded.source_tree, "
            "category = excluded.category, "
            "testing = excluded.testing, "
            "suite = excluded.suite, "
            "component = excluded.component, "
            "architecture = excluded.architecture, "
            "origin = excluded.origin, "
            "label = excluded.label, "
            "codename = excluded.codename, "
            "date = excluded.date, "
            "valid_until = excluded.valid_until, "
            "description = excluded.description ",
            (
                repo.name,
                repo.realname,
                repo.source_tree,
                repo.category,
                repo.testing,
                repo.suite,
                repo.component,
                repo.architecture,
                rel.get("Origin"),
                rel.get("Label"),
                rel.get("Codename"),
                rel_date,
                rel_valid,
                rel.get("Description"),
            ),
        )
    for repo in repo_dict.values():
        cur.execute(
            "UPDATE dpkg_repos SET origin = null, label = null, "
            "codename = null, date = null, valid_until = null, description = null "
            "WHERE name = %s",
            (repo.name,),
        )
        cur.execute(
            "DELETE FROM dpkg_package_dependencies WHERE repo = %s", (repo.name,)
        )
        cur.execute("DELETE FROM dpkg_package_duplicate WHERE repo = %s", (repo.name,))
        cur.execute("DELETE FROM dpkg_packages WHERE repo = %s", (repo.name,))
    cur.close()
    db.commit()
    return result_repos


_relationship_fields = (
    "depends",
    "pre-depends",
    "recommends",
    "suggests",
    "breaks",
    "conflicts",
    "provides",
    "replaces",
    "enhances",
)


def package_update(db, mirror, repo, path, size, sha256, local=False):
    logging.info(repo.name)
    url = urllib.parse.urljoin(mirror, path)
    content = download_catalog(url, local)
    if len(content) != size:
        logging.warning("%s size %d != %d", url, len(content), size)
    elif hashlib.sha256(content).hexdigest() != sha256:
        logging.warning("%s sha256 mismatch", url)
    pkgs = lzma.decompress(content).decode("utf-8")
    packages = {}
    cur = db.cursor()
    cur.execute("DELETE FROM dpkg_package_duplicate WHERE repo = %s", (repo.name,))
    cur.execute(
        "SELECT package, version, architecture, repo FROM dpkg_packages "
        "WHERE repo = %s",
        (repo.name,),
    )
    packages_old = set(cur.fetchall())
    for pkg in deb822.Packages.iter_paragraphs(pkgs):
        name = pkg["Package"]
        arch = pkg["Architecture"]
        ver = pkg["Version"]
        pkgtuple = (name, ver, arch, repo.name)
        pkginfo = (
            name,
            ver,
            arch,
            repo.name,
            pkg.get("Maintainer"),
            int(pkg["Installed-Size"]) if "Installed-Size" in pkg else None,
            pkg["Filename"],
            int(pkg["Size"]),
            pkg.get("SHA256"),
        )
        if pkgtuple in packages:
            logging.warning("duplicate package: %r", pkgtuple)
            cur.execute(
                "INSERT INTO dpkg_package_duplicate VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (repo, filename) "
                "DO UPDATE SET "
                "package = excluded.package, "
                "version = excluded.version, "
                "architecture = excluded.architecture, "
                "maintainer = excluded.maintainer, "
                "installed_size = excluded.installed_size, "
                "size = excluded.size, "
                "sha256 = excluded.sha256, ",
                packages[pkgtuple],
            )
            cur.execute(
                "INSERT INTO dpkg_package_duplicate VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (repo, filename) "
                "DO UPDATE SET "
                "package = excluded.package, "
                "version = excluded.version, "
                "architecture = excluded.architecture, "
                "maintainer = excluded.maintainer, "
                "installed_size = excluded.installed_size, "
                "size = excluded.size, "
                "sha256 = excluded.sha256, ",
                pkginfo,
            )
            if pkg["Filename"] < packages[pkgtuple][6]:
                continue
        packages[pkgtuple] = pkginfo
        cur.execute(
            "INSERT INTO dpkg_packages VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (package, version, architecture, repo) "
            "DO UPDATE SET "
            "maintainer = excluded.maintainer, "
            "installed_size = excluded.installed_size, "
            "filename = excluded.filename, "
            "size = excluded.size, "
            "sha256 = excluded.sha256",
            pkginfo,
        )

        cur.execute(
            "SELECT relationship FROM dpkg_package_dependencies"
            " WHERE package = %s AND version = %s AND architecture = %s AND repo = %s",
            (name, ver, arch, repo.name),
        )
        oldrels = frozenset(row[0] for row in cur.fetchall())
        newrels = set()
        for rel in _relationship_fields:
            if rel in pkg:
                cur.execute(
                    "INSERT INTO dpkg_package_dependencies VALUES (%s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (package, version, architecture, repo, relationship) "
                    "DO UPDATE SET value = excluded.value",
                    (name, ver, arch, repo.name, rel, pkg[rel]),
                )
                newrels.add(rel)
        for rel in oldrels.difference(newrels):
            cur.execute(
                "DELETE FROM dpkg_package_dependencies"
                " WHERE package = %s AND version = %s AND architecture = %s"
                " AND repo = %s AND relationship = %s",
                (name, ver, arch, repo.name, rel),
            )
    for pkg in packages_old.difference(packages.keys()):
        cur.execute(
            "DELETE FROM dpkg_packages WHERE package = %s AND version = %s"
            " AND architecture = %s AND repo = %s",
            pkg,
        )
        cur.execute(
            "DELETE FROM dpkg_package_dependencies WHERE package = %s"
            " AND version = %s AND architecture = %s AND repo = %s",
            pkg,
        )
    cur.close()
    db.commit()


SQL_COUNT_REPO = """
INSERT INTO dpkg_repo_stats
SELECT c0.repo repo, pkgcount, ghost, lagging, missing, coalesce(olddebcnt, 0)
FROM (
SELECT
  dpkg_repos.name repo, dpkg_repos.realname reponame,
  dpkg_repos.testing testing, dpkg_repos.category category,
  count(packages.name) pkgcount
FROM dpkg_repos
LEFT JOIN (
    SELECT DISTINCT dp.package, dp.repo, dr.realname reponame, dr.architecture
    FROM dpkg_packages dp
    LEFT JOIN dpkg_repos dr ON dr.name = dp.repo
  ) dpkg
  ON dpkg.repo = dpkg_repos.name
LEFT JOIN packages
  ON packages.name = dpkg.package
LEFT JOIN package_spec spabhost
  ON spabhost.package = packages.name AND spabhost.key = 'ABHOST'
WHERE packages.name IS NULL
OR ((coalesce(spabhost.value, '') = 'noarch') = (dpkg.architecture = 'noarch'))
GROUP BY dpkg_repos.name
) c0
LEFT JOIN (
SELECT
  dpkg_repos.name repo, dpkg_repos.realname reponame,
  dpkg_repos.testing testing, dpkg_repos.category category,
  sum(CASE WHEN packages.name IS NULL THEN 1 ELSE 0 END) ghost
FROM dpkg_repos
LEFT JOIN (
    SELECT DISTINCT dp.package, dp.repo, dr.realname reponame, dr.architecture
    FROM dpkg_packages dp
    LEFT JOIN dpkg_repos dr ON dr.name = dp.repo
  ) dpkg
  ON dpkg.repo = dpkg_repos.name
LEFT JOIN packages
  ON packages.name = regexp_replace(dpkg.package, '-dbg$', '')
LEFT JOIN package_spec spabhost
  ON spabhost.package = packages.name AND spabhost.key = 'ABHOST'
WHERE packages.name IS NULL
OR ((coalesce(spabhost.value, '') = 'noarch') = (dpkg.architecture = 'noarch'))
GROUP BY dpkg_repos.name
) c1 ON c1.repo = c0.repo
LEFT JOIN (
SELECT
  dpkg.repo repo, dpkg.reponame reponame,
  sum(CASE WHEN comparable_dpkgver(pkgver.fullver) > comparable_dpkgver(dpkg.version) THEN 1 ELSE 0 END) lagging
FROM packages
INNER JOIN (
    SELECT
      package, branch,
      ((CASE WHEN coalesce(epoch, '') = '' THEN '' ELSE epoch || ':' END) ||
       version || (CASE WHEN coalesce(release, '') IN ('', '0') THEN '' ELSE '-'
       || release END)) fullver
    FROM package_versions
  ) pkgver
  ON pkgver.package = packages.name
INNER JOIN trees ON trees.name = packages.tree
LEFT JOIN package_spec spabhost
  ON spabhost.package = packages.name AND spabhost.key = 'ABHOST'
LEFT JOIN (
    SELECT
      dp_d.name package, dr.name repo, dr.realname reponame,
      max_dpkgver(dp.version) AS version, dr.category category,
      dr.architecture architecture, dr.suite branch
    FROM packages dp_d
    LEFT JOIN dpkg_packages dp ON dp.package = dp_d.name
    INNER JOIN dpkg_repos dr ON dp.repo = dr.name
    GROUP BY dp_d.name, dr.name
  ) dpkg ON dpkg.package = packages.name
WHERE pkgver.branch = dpkg.branch
  AND ((coalesce(spabhost.value, '') = 'noarch') = (dpkg.architecture = 'noarch'))
  AND dpkg.repo IS NOT null
  AND (dpkg.version IS NOT null OR (dpkg.category = 'bsp') = (trees.category = 'bsp'))
GROUP BY dpkg.repo, dpkg.reponame
) c2 ON c2.repo=c1.repo
LEFT JOIN (
SELECT reponame, sum(CASE WHEN dpp IS NULL THEN 1 ELSE 0 END) missing
FROM (
  SELECT
    packages.name package, dr.realname reponame, dr.category category,
    max(dp.package) dpp
  FROM packages
  INNER JOIN dpkg_repos dr ON TRUE
  INNER JOIN trees ON trees.name = packages.tree
  INNER JOIN package_versions pv
    ON pv.package=packages.name AND pv.branch=trees.mainbranch
    AND pv.version IS NOT NULL
  LEFT JOIN package_spec spabhost
    ON spabhost.package = packages.name AND spabhost.key = 'ABHOST'
  LEFT JOIN dpkg_packages dp ON dp.package = packages.name AND dp.repo = dr.name
  WHERE ((coalesce(spabhost.value, '') = 'noarch') = (dr.architecture = 'noarch'))
  AND dr.category != 'overlay'
  AND (dp.package IS NOT null OR (dr.category = 'bsp') = (trees.category = 'bsp'))
  GROUP BY packages.name, dr.realname, dr.category
) AS pkgs
GROUP BY reponame
) c3 ON c3.reponame=c1.reponame AND c1.testing=0
LEFT JOIN (
  SELECT repo, count(repo) olddebcnt
  FROM (
    SELECT dp.repo
    FROM dpkg_packages dp
    INNER JOIN dpkg_repos dr ON dr.name=dp.repo
    LEFT JOIN (
      SELECT DISTINCT ON (package, architecture, repo)
          package, version, repo, architecture
      FROM dpkg_packages
      ORDER BY package, architecture, repo ASC, _vercomp DESC
    ) dpnew ON dp.package = dpnew.package AND
               dp.version = dpnew.version AND
               dp.architecture = dpnew.architecture AND
               dp.repo = dpnew.repo
    LEFT JOIN packages ON packages.name = dp.package
    LEFT JOIN package_spec spabhost
      ON spabhost.package = dp.package AND spabhost.key = 'ABHOST'
    LEFT JOIN (
      SELECT DISTINCT ON (dp.package, noarch)
        dp.package, (dr.architecture = 'noarch') noarch,
        dp.version AS version
      FROM dpkg_packages dp
               INNER JOIN dpkg_repos dr ON dr.name=dp.repo
      ORDER BY dp.package, (dr.architecture = 'noarch') ASC, _vercomp DESC
    ) dparch ON dparch.package=dp.package
    AND (dr.architecture != 'noarch') = dparch.noarch
    AND dparch.version=dpnew.version
    WHERE (dpnew.package IS NULL OR packages.name IS NULL
    OR ((dr.architecture = 'noarch') = (coalesce(spabhost.value, 'noarch') != 'noarch')
      AND dparch.package IS NULL))
    UNION ALL
    SELECT repo FROM dpkg_package_duplicate
  ) q1
  GROUP BY repo
) c4 ON c4.repo=c1.repo
ORDER BY c1.category, c1.reponame, c1.testing
ON CONFLICT (repo)
DO UPDATE SET
packagecnt = excluded.packagecnt,
ghostcnt = excluded.ghostcnt,
laggingcnt = excluded.laggingcnt,
missingcnt = excluded.missingcnt,
oldcnt = excluded.oldcnt
"""


def stats_update(db):
    cur = db.cursor()
    cur.execute(SQL_COUNT_REPO)
    db.commit()


def update(db, mirror, branches=None, arch=None, local=False, force=False):
    branches = frozenset(branches if branches else ())
    for suite, repos in REPOS.items():
        if branches and suite not in branches:
            continue
        pkgrepos = suite_update(db, mirror, suite, repos, local, force)
        for repo, path, size, sha256 in pkgrepos.values():
            if arch and repo.architecture != arch:
                continue
            package_update(db, mirror, repo, path, size, sha256, local)


def update_sources_list(
    db, filename, branches=None, arch=None, local=False, force=False
):
    with open(filename, "r", encoding="utf-8") as f:
        for ln in f:
            if ln[0] == "#":
                continue
            hashpos = ln.find("#")
            if hashpos != -1:
                ln = ln[:hashpos]
            fields = ln.strip().split()
            if not fields:
                continue
            elif fields[0] != "deb":
                continue
            elif branches and fields[2] not in branches:
                continue
            mirror = _url_slash(fields[1])
            pkgrepos = suite_update(db, mirror, fields[2], None, local, force)
            for repo, path, size, sha256 in pkgrepos.values():
                if arch and repo.architecture != arch:
                    continue
                package_update(db, mirror, repo, path, size, sha256, local)


def main(argv):
    parser = argparse.ArgumentParser(description="Get package info from DPKG sources.")
    parser.add_argument(
        "-l", "--local", help="Try local apt cache", action="store_true"
    )
    parser.add_argument("-f", "--force", help="Force update", action="store_true")
    parser.add_argument(
        "-n", "--no-stats", help="Don't calculate package stats", action="store_true"
    )
    parser.add_argument(
        "-b",
        "--branch",
        help="Only get this branch, can be specified multiple times",
        action="append",
    )
    parser.add_argument("-a", "--arch", help="Only get this architecture")
    parser.add_argument(
        "-m",
        "--mirror",
        help="Set mirror location, https is recommended. "
        "This overrides environment variable REPO_MIRROR. ",
        default=os.environ.get("REPO_MIRROR", "https://repo.aosc.io/debs"),
    )
    parser.add_argument(
        "-s", "--sources-list", help="Use specified sources.list file as repo list."
    )
    parser.add_argument("dburl", help="abbs database url")
    args = parser.parse_args(argv)

    db = psycopg2.connect(args.dburl)
    init_db(db, not args.no_stats)
    if args.sources_list:
        update_sources_list(
            db, args.sources_list, args.branch, args.arch, args.local, args.force
        )
    else:
        update(
            db, _url_slash(args.mirror), args.branch, args.arch, args.local, args.force
        )
    if not args.no_stats:
        stats_update(db)
    # refresh materialized view
    cur = db.cursor()
    cur.execute("REFRESH MATERIALIZED VIEW v_dpkg_packages_new;")
    db.commit()


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
