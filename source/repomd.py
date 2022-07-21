import io
import os
import bz2
import gzip
import lzma
import shutil
import sqlite3
import pathlib
import tempfile
import datetime
import urllib.parse
import urllib.request
import defusedxml.lxml

_ns = {
    'common': 'http://linux.duke.edu/metadata/common',
    'repo': 'http://linux.duke.edu/metadata/repo',
    'rpm': 'http://linux.duke.edu/metadata/rpm'
}


def load(baseurl, use_sql=False):
    # parse baseurl to allow manipulating the path
    base = urllib.parse.urlparse(baseurl)
    path = pathlib.PurePosixPath(base.path)

    # first we must get the repomd.xml file
    repomd_path = path / 'repodata' / 'repomd.xml'
    repomd_url = base._replace(path=str(repomd_path)).geturl()

    # download and parse repomd.xml
    with urllib.request.urlopen(repomd_url) as response:
        repomd_xml = defusedxml.lxml.fromstring(response.read())

    # determine the location of *primary.xml.gz
    primary_file = 'primary_db' if use_sql else 'primary'
    primary_element = repomd_xml.find(f'repo:data[@type="{primary_file}"]/repo:location', namespaces=_ns)
    primary_path = path / primary_element.get('href')
    primary_url = base._replace(path=str(primary_path)).geturl()

    # download and parse *-primary.xml
    if not use_sql:
        with urllib.request.urlopen(primary_url) as response:
            with io.BytesIO(response.read()) as compressed:
                with gzip.GzipFile(fileobj=compressed) as uncompressed:
                    data = defusedxml.lxml.fromstring(uncompressed.read())

    return RepoSQL(baseurl, primary_url) if use_sql else Repo(baseurl, data)


class Repo:
    """A dnf/yum repository."""

    __slots__ = ['baseurl', '_metadata']

    def __init__(self, baseurl, metadata):
        self.baseurl = baseurl
        self._metadata = metadata

    def __repr__(self):
        return f'<{self.__class__.__name__}: "{self.baseurl}">'

    def __str__(self):
        return self.baseurl

    def __len__(self):
        return int(self._metadata.get('packages'))

    def __iter__(self):
        for element in self._metadata:
            yield Package(element)

    def find(self, name):
        results = self._metadata.findall(f'common:package[common:name="{name}"]', namespaces=_ns)
        if results:
            return Package(results[-1])
        else:
            return None

    def findall(self, name):
        return [
            Package(element)
            for element in self._metadata.findall(f'common:package[common:name="{name}"]', namespaces=_ns)
        ]


class Package:
    """An RPM package from a repository."""

    __slots__ = ['_element']

    def __init__(self, element):
        self._element = element

    @property
    def name(self):
        return self._element.findtext('common:name', namespaces=_ns)

    @property
    def arch(self):
        return self._element.findtext('common:arch', namespaces=_ns)

    @property
    def summary(self):
        return self._element.findtext('common:summary', namespaces=_ns)

    @property
    def description(self):
        return self._element.findtext('common:description', namespaces=_ns)

    @property
    def packager(self):
        return self._element.findtext('common:packager', namespaces=_ns)

    @property
    def url(self):
        return self._element.findtext('common:url', namespaces=_ns)

    @property
    def license(self):
        return self._element.findtext('common:format/rpm:license', namespaces=_ns)

    @property
    def vendor(self):
        return self._element.findtext('common:format/rpm:vendor', namespaces=_ns)

    @property
    def buildhost(self):
        return self._element.findtext('common:format/rpm:buildhost', namespaces=_ns)

    @property
    def sourcerpm(self):
        return self._element.findtext('common:format/rpm:sourcerpm', namespaces=_ns)

    @property
    def build_time(self):
        build_time = self._element.find('common:time', namespaces=_ns).get('build')
        return datetime.datetime.fromtimestamp(int(build_time))

    @property
    def package_size(self):
        package_size = self._element.find('common:size', namespaces=_ns).get('package')
        return int(package_size)

    @property
    def installed_size(self):
        installed_size = self._element.find('common:size', namespaces=_ns).get('installed')
        return int(installed_size)

    @property
    def archive_size(self):
        archive_size = self._element.find('common:size', namespaces=_ns).get('archive')
        return int(archive_size)

    @property
    def location(self):
        return self._element.find('common:location', namespaces=_ns).get('href')

    @property
    def _version_info(self):
        return self._element.find('common:version', namespaces=_ns)

    @property
    def epoch(self):
        return self._version_info.get('epoch')

    @property
    def version(self):
        return self._version_info.get('ver')

    @property
    def release(self):
        return self._version_info.get('rel')

    @property
    def vr(self):
        version_info = self._version_info
        v = version_info.get('ver')
        r = version_info.get('rel')
        return f'{v}-{r}'

    @property
    def nvr(self):
        return f'{self.name}-{self.vr}'

    @property
    def evr(self):
        version_info = self._version_info
        e = version_info.get('epoch')
        v = version_info.get('ver')
        r = version_info.get('rel')
        if int(e):
            return f'{e}:{v}-{r}'
        else:
            return f'{v}-{r}'

    @property
    def nevr(self):
        return f'{self.name}-{self.evr}'

    @property
    def nevra(self):
        return f'{self.nevr}.{self.arch}'

    @property
    def _nevra_tuple(self):
        return self.name, self.epoch, self.version, self.release, self.arch

    def __eq__(self, other):
        return self._nevra_tuple == other._nevra_tuple

    def __hash__(self):
        return hash(self._nevra_tuple)

    def __repr__(self):
        return f'<{self.__class__.__name__}: "{self.nevra}">'


# Compreddion part
def uncompress_bz(_source_path, _dest_path):
    """
        Uncompress file from bz.

        :param _source_path:        str path to the archive.
        :param _dest_path:          str path to the uncompressed file.
    """
    with bz2.open(_source_path, 'rb') as f_source:
        with open(_dest_path, 'wb') as f_dest:
            f_dest.write(f_source.read())


def uncompress_gz(_source_path, _dest_path):
    """
        Uncompress file from gz.

        :param _source_path:        str path to the archive.
        :param _dest_path:          str path to the uncompressed file.
    """
    with gzip.open(_source_path, "rb") as f_source:
        with open(_dest_path, "wb") as f_dest:
            f_dest.write(f_source.read())


def uncompress_xz(_source_path, _dest_path):
    """
        Uncompress file from xz.

        :param _source_path:        str path to the archive.
        :param _dest_path:          str path to the uncompressed file.
    """
    with lzma.open(_source_path, "rb") as f_source:
        with open(_dest_path, "wb") as f_dest:
            f_dest.write(f_source.read())


def uncompress(source_path, dest_path, compression_type):
    """
        Uncompress archive.

        :param source_path:        str path to the archive.
        :param dest_path:          str path to the uncompressed file.
        :param compression_type:   str compression type.
    """

    uncompress_dict = {
        'bz2': uncompress_bz,
        'bz': uncompress_bz,
        'gz': uncompress_gz,
        'xz': uncompress_xz
    }

    uncompress_dict[compression_type](source_path, dest_path)


class RepoSQL:
    """A dnf/yum repository."""

    __slots__ = ['baseurl', '_metadata', 'tempdir', 'db_path', 'cursor', 'compression_type']

    def __init__(self, baseurl, primary_url):
        self.baseurl = baseurl
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tempdir.name, 'primary.sqlite')
        self.compression_type = primary_url.split('.')[-1] if primary_url.split('.')[-1] != 'sqlite' else None

        if self.compression_type:
            # Download compressed primary.sqlite
            compressed_path = f"{self.db_path}.{self.compression_type}"
            with urllib.request.urlopen(primary_url) as response, open(compressed_path, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
            # Decompress primary.sqlite
            try:
                uncompress(source_path=compressed_path,
                           dest_path=self.db_path,
                           compression_type=self.compression_type)
            except KeyError:
                raise ValueError(f'Unknown compression type: {self.compression_type}')
        else:
            # Download primary.sqlite
            with urllib.request.urlopen(primary_url) as response, open(self.db_path, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)

        self.cursor = sqlite3.connect(self.db_path).cursor()

    def __del__(self):
        self.tempdir._finalizer()

    def __repr__(self):
        return f'<{self.__class__.__name__}: "{self.baseurl}">'

    def __str__(self):
        return self.baseurl

    def __len__(self):
        return int(self.cursor.execute('SELECT COUNT(*) FROM packages;').fetchall()[0][0])

    def __iter__(self):
        self.cursor.execute('SELECT * FROM packages;')
        for element in self.cursor:
            yield PackageSQL(*element)

    def find(self, name):
        results = self.cursor.execute('SELECT * FROM packages WHERE name=?;', (name,)).fetchall()
        if results:
            return PackageSQL(*results[-1])
        else:
            return None

    def findall(self, name):
        return [
            PackageSQL(*element)
            for element in self.cursor.execute('SELECT * FROM packages WHERE name=?;', (name,)).fetchall()
        ]


class PackageSQL:
    """An RPM package from a repository."""

    __slots__ = ['_pkgKey', '_pkgId', 'name', 'arch', 'version', 'epoch', 'release', 'summary', 'description', 'url',
                 'time_file', 'build_time', 'license', 'vendor', 'rpm_group', 'buildhost', 'sourcerpm',
                 'rpm_header_start', 'rpm_header_end', 'packager', 'package_size', 'installed_size', 'archive_size',
                 'location', 'location_base', 'checksum_type']

    def __init__(self, pkgKey, pkgId, name, arch, version, epoch, release, summary, description, url, time_file,
                 time_build, rpm_license, rpm_vendor, rpm_group, rpm_buildhost, rpm_sourcerpm, rpm_header_start,
                 rpm_header_end, rpm_packager, size_package, size_installed, size_archive, location_href, location_base,
                 checksum_type):

        self._pkgKey = pkgKey
        self._pkgId = pkgId
        self.name = name
        self.arch = arch
        self.version = version
        self.epoch = epoch
        self.release = release
        self.summary = summary
        self.description = description
        self.url = url
        self.time_file = time_file
        self.build_time = datetime.datetime.fromtimestamp(int(time_build)) if time_build else None
        self.license = rpm_license
        self.vendor = rpm_vendor
        self.rpm_group = rpm_group
        self.buildhost = rpm_buildhost
        self.sourcerpm = rpm_sourcerpm
        self.rpm_header_start = rpm_header_start
        self.rpm_header_end = rpm_header_end
        self.packager = rpm_packager
        self.package_size = int(size_package) if size_package else None
        self.installed_size = int(size_installed) if size_installed else None
        self.archive_size = int(size_archive) if size_archive else None
        self.location = location_href
        self.location_base = location_base
        self.checksum_type = checksum_type

    @property
    def vr(self):
        return f'{self.version}-{self.release}'

    @property
    def nvr(self):
        return f'{self.name}-{self.vr}'

    @property
    def evr(self):
        if int(self.epoch):
            return f'{self.epoch}:{self.version}-{self.release}'
        else:
            return f'{self.version}-{self.release}'

    @property
    def nevr(self):
        return f'{self.name}-{self.evr}'

    @property
    def nevra(self):
        return f'{self.nevr}.{self.arch}'

    @property
    def _nevra_tuple(self):
        return self.name, self.epoch, self.version, self.release, self.arch

    def __eq__(self, other):
        return self._nevra_tuple == other._nevra_tuple

    def __hash__(self):
        return hash(self._nevra_tuple)

    def __repr__(self):
        return f'<{self.__class__.__name__}: "{self.nevra}">'
