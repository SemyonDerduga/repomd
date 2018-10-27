from copy import copy
from datetime import datetime
from unittest import mock

import pytest
from lxml import etree

import repomd


def load_repodata(path):
    with open(f'{path}/repomd.xml', 'rb') as f:
        repomd_xml = f.read()
    with open(f'{path}/primary.xml.gz', 'rb') as f:
        primary_xml = f.read()
    return (repomd_xml, primary_xml)


@pytest.fixture
@mock.patch('repomd.urlopen')
def repo(mock_urlopen):
    mock_urlopen.return_value.__enter__.return_value.read.side_effect = load_repodata('tests/data/repo/repodata')
    return repomd.load('https://example.com')


@pytest.fixture
@mock.patch('repomd.urlopen')
def empty_repo(mock_urlopen):
    mock_urlopen.return_value.__enter__.return_value.read.side_effect = load_repodata('tests/data/empty_repo/repodata')
    return repomd.load('https://example.com')


@pytest.fixture
def chicken(repo):
    return repo.find('chicken')


@pytest.fixture
def brisket(repo):
    return repo.find('brisket')


def test_repo(repo):
    assert repo.baseurl == 'https://example.com'
    assert isinstance(repo._metadata, etree._Element)


def test_repo_repr(repo):
    assert repr(repo) == '<Repo: "https://example.com">'


def test_repo_str(repo):
    assert str(repo) == 'https://example.com'


def test_repo_len(repo, empty_repo):
    assert len(repo) == 5
    assert len(empty_repo) == 0


def test_find(repo):
    package = repo.find('non-existent')
    assert package is None
    package = repo.find('chicken')
    assert isinstance(package, repomd.Package)


def test_findall(repo):
    packages = repo.findall('non-existent')
    assert packages == []
    packages = repo.findall('chicken')
    assert any(packages)
    for package in packages:
        assert isinstance(package, repomd.Package)


def test_iter(repo):
    for package in repo:
        assert isinstance(package, repomd.Package)


def test_package(chicken):
    assert repr(chicken) == '<Package: "chicken-2.2.10-1.fc27.noarch">'
    assert chicken.name == 'chicken'
    assert chicken.arch == 'noarch'
    assert chicken.summary == 'Chicken'
    assert chicken.description == 'Chicken.'
    assert chicken.packager == 'Carl'
    assert chicken.url == 'https://example.com/chicken'
    assert chicken.license == 'BBQ'
    assert chicken.vendor == "Carl's BBQ"
    assert chicken.sourcerpm == 'chicken-2.2.10-1.fc27.src.rpm'
    assert chicken.build_time == datetime.fromtimestamp(1525208602)
    assert chicken.location == 'chicken-2.2.10-1.fc27.noarch.rpm'
    assert chicken.epoch == '0'
    assert chicken.version == '2.2.10'
    assert chicken.release == '1.fc27'
    assert chicken.vr == '2.2.10-1.fc27'
    assert chicken.nvr == 'chicken-2.2.10-1.fc27'
    assert chicken.evr == '2.2.10-1.fc27'
    assert chicken.nevr == 'chicken-2.2.10-1.fc27'
    assert chicken.nevra == 'chicken-2.2.10-1.fc27.noarch'
    assert chicken.nevra_tuple == ('chicken', '0', '2.2.10', '1.fc27', 'noarch')


def test_package_with_epoch(brisket):
    assert repr(brisket) == '<Package: "brisket-1:5.1.1-1.fc27.noarch">'
    assert brisket.name == 'brisket'
    assert brisket.arch == 'noarch'
    assert brisket.summary == 'Brisket'
    assert brisket.description == 'Brisket.'
    assert brisket.packager == 'Carl'
    assert brisket.url == 'https://example.com/brisket'
    assert brisket.license == 'BBQ'
    assert brisket.vendor == "Carl's BBQ"
    assert brisket.sourcerpm == 'brisket-5.1.1-1.fc27.src.rpm'
    assert brisket.build_time == datetime.fromtimestamp(1525208602)
    assert brisket.location == 'brisket-5.1.1-1.fc27.noarch.rpm'
    assert brisket.epoch == '1'
    assert brisket.version == '5.1.1'
    assert brisket.release == '1.fc27'
    assert brisket.vr == '5.1.1-1.fc27'
    assert brisket.nvr == 'brisket-5.1.1-1.fc27'
    assert brisket.evr == '1:5.1.1-1.fc27'
    assert brisket.nevr == 'brisket-1:5.1.1-1.fc27'
    assert brisket.nevra == 'brisket-1:5.1.1-1.fc27.noarch'
    assert brisket.nevra_tuple == ('brisket', '1', '5.1.1', '1.fc27', 'noarch')


def test_package_equals_its_copy(chicken):
    copied_chicken = copy(chicken)
    assert chicken is chicken
    assert chicken == chicken
    assert chicken is not copied_chicken
    assert chicken == copied_chicken


def test_packages_can_be_used_as_dict_keys(chicken, brisket):
    d = {chicken: 'chicken', brisket: 'brisket'}
    copied_chicken = copy(chicken)
    assert d[copied_chicken] == 'chicken'


def test_equal_packages_work_in_set(chicken, brisket):
    copied_chicken = copy(chicken)
    copied_brisket = copy(brisket)
    assert len({chicken, brisket, copied_chicken, copied_brisket}) == 2
