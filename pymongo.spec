%if 0%{?fedora} > 12 || 0%{?rhel} > 6
# This code doesn't automatically convert cleanly, leaving it so we can try
# again later.
# http://jira.mongodb.org/browse/PYTHON-84
%global with_python3 0
%else
%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)")}
%endif

# Fix private-shared-object-provides error
%{?filter_setup:
%filter_provides_in %{python_sitearch}.*\.so$
%filter_setup
}

Name:           pymongo
Version:        2.0.1
Release:        1%{?dist}
Summary:        Python driver for MongoDB

Group:          Development/Languages
# All code is ASL 2.0 except bson/time64*.{c,h} which is MIT
License:        ASL 2.0 and MIT
URL:            http://api.mongodb.org/python
Source0:        http://pypi.python.org/packages/source/p/pymongo/%{name}-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
Requires:       python-bson = %{version}-%{release}

BuildRequires:  python2-devel
BuildRequires:  python-nose
BuildRequires:  python-setuptools

%if 0%{?with_python3}
BuildRequires:  python3-devel
BuildRequires:  python3-setuptools
BuildRequires:  /usr/bin/2to3
%endif # if with_python3

# Mongodb must run on a little-endian CPU (see bug #630898)
ExcludeArch:    ppc ppc64 %{sparc} s390 s390x

%description
The Python driver for MongoDB.

%if 0%{?with_python3}
%package -n python3-pymongo
Summary:        Python driver for MongoDB
Group:          Development/Languages
Requires:       python3-bson = %{version}-%{release}

%description -n python3-pymongo
The Python driver for MongoDB.
%endif # with_python3

%package gridfs
Summary:        Python GridFS driver for MongoDB
Group:          Development/Libraries
Requires:       %{name} = %{version}-%{release}

%description gridfs
GridFS is a storage specification for large objects in MongoDB.

%if 0%{?with_python3}
%package -n python3-pymongo-gridfs
Summary:        Python GridFS driver for MongoDB
Group:          Development/Libraries
Requires:       %{name} = %{version}-%{release}

%description -n python3-pymongo-gridfs
GridFS is a storage specification for large objects in MongoDB.
%endif # with_python3

%package -n python-bson
Summary:        Python bson library
Group:          Development/Libraries

%description -n python-bson
BSON is a binary-encoded serialization of JSON-like documents. BSON is designed
to be lightweight, traversable, and efficient. BSON, like JSON, supports the
embedding of objects and arrays within other objects and arrays.

%if 0%{?with_python3}
%package -n python3-bson
Summary:        Python bson library
Group:          Development/Libraries

%description -n python3-bson
BSON is a binary-encoded serialization of JSON-like documents. BSON is designed
to be lightweight, traversable, and efficient. BSON, like JSON, supports the
embedding of objects and arrays within other objects and arrays.
%endif # with_python3

%prep
%setup -q

%if 0%{?with_python3}
rm -rf %{py3dir}
cp -a . %{py3dir}
2to3 --write --nobackups %{py3dir}
%endif # with_python3

%build
CFLAGS="%{optflags}" %{__python} setup.py build

%if 0%{?with_python3}
pushd %{py3dir}
CFLAGS="%{optflags}" %{__python3} setup.py build
popd
%endif # with_python3

%install
rm -rf %{buildroot}
%{__python} setup.py install -O1 --skip-build --root %{buildroot}
# Fix non-standard-executable-perm error
chmod 755 %{buildroot}%{python_sitearch}/%{name}/_cmessage.so
chmod 755 %{buildroot}%{python_sitearch}/bson/_cbson.so

%if 0%{?with_python3}
pushd %{py3dir}
%{__python3} setup.py install -O1 --skip-build --root %{buildroot}
# Fix non-standard-executable-perm error
chmod 755 %{buildroot}%{python3_sitearch}/%{name}/_cmessage.so
chmod 755 %{buildroot}%{python3_sitearch}/bson/_cbson.so
popd
%endif # with_python3

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%doc LICENSE pymongo.egg-info/PKG-INFO README.rst doc
%{python_sitearch}/%{name}
%{python_sitearch}/%{name}-%{version}-*.egg-info

%if 0%{?with_python3}
%files -n python3-pymongo
%defattr(-,root,root,-)
%doc LICENSE pymongo.egg-info/PKG-INFO README.rst doc
%{python3_sitearch}/%{name}
%{python3_sitearch}/%{name}-%{version}-*.egg-info
%endif # with_python3

%files gridfs
%defattr(-,root,root,-)
%{python_sitearch}/gridfs

%if 0%{?with_python3}
%files -n python3-pymongo-gridfs
%defattr(-,root,root,-)
%{python3_sitearch}/gridfs
%endif # with_python3

%files -n python-bson
%defattr(-,root,root,-)
%doc LICENSE
%{python_sitearch}/bson

%if 0%{?with_python3}
%files -n python3-bson
%defattr(-,root,root,-)
%doc LICENSE
%{python3_sitearch}/bson
%endif # with_python3

%check
 exclude='(^test_collection$'
exclude+='|^test_connection$'
exclude+='|^test_cursor$'
exclude+='|^test_database$'
exclude+='|^test_grid_file$'
exclude+='|^test_gridfs$'
exclude+='|^test_master_slave_connection$'
exclude+='|^test_paired$'
exclude+='|^test_pooling$'
exclude+='|^test_pymongo$'
exclude+='|^test_son_manipulator$'
exclude+='|^test_threads$'
exclude+=')'
# Exclude tests that require an active MongoDB connection
pushd test
truncate --size=0 __init__.py
nosetests --exclude="$exclude"
popd

%changelog
* Thu Sep 22 2011 Pau Aliagas <pau@newtral.org> - 2.0.1-1
- Update to 2.0.1

* Sun Jul 24 2011 Silas Sewell <silas@sewell.org> - 1.11-1
- Update to 1.11

* Tue Feb 08 2011 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 1.9-6
- Rebuilt for https://fedoraproject.org/wiki/Fedora_15_Mass_Rebuild

* Thu Nov 18 2010 Dan Horák <dan[at]danny.cz> - 1.9-5
- add ExcludeArch to match mongodb package

* Tue Oct 26 2010 Silas Sewell <silas@sewell.ch> - 1.9-4
- Add comment about multi-license

* Thu Oct 21 2010 Silas Sewell <silas@sewell.ch> - 1.9-3
- Fixed tests so they actually run
- Change python-devel to python2-devel

* Wed Oct 20 2010 Silas Sewell <silas@sewell.ch> - 1.9-2
- Add check section
- Use correct .so filter
- Added python3 stuff (although disabled)

* Tue Sep 28 2010 Silas Sewell <silas@sewell.ch> - 1.9-1
- Update to 1.9

* Tue Sep 28 2010 Silas Sewell <silas@sewell.ch> - 1.8.1-1
- Update to 1.8.1

* Sat Dec 05 2009 Silas Sewell <silas@sewell.ch> - 1.1.2-1
- Initial build
