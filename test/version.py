# Copyright 2009-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Some tools for running tests based on MongoDB server version."""


class Version(tuple):
    def __new__(cls, *version):
        padded_version = cls._padded(version, 4)
        return super(Version, cls).__new__(cls, tuple(padded_version))

    @classmethod
    def _padded(cls, iter, length, padding=0):
        l = list(iter)
        if len(l) < length:
            for _ in range(length - len(l)):
                l.append(padding)
        return l

    @classmethod
    def from_string(cls, version_string):
        mod = 0
        bump_patch_level = False
        if version_string.endswith("+"):
            version_string = version_string[0:-1]
            mod = 1
        elif version_string.endswith("-pre-"):
            version_string = version_string[0:-5]
            mod = -1
        elif version_string.endswith("-"):
            version_string = version_string[0:-1]
            mod = -1
        # Deal with '-rcX' substrings
        if "-rc" in version_string:
            version_string = version_string[0 : version_string.find("-rc")]
            mod = -1
        # Deal with git describe generated substrings
        elif "-" in version_string:
            version_string = version_string[0 : version_string.find("-")]
            mod = -1
            bump_patch_level = True

        version = [int(part) for part in version_string.split(".")]
        version = cls._padded(version, 3)
        # Make from_string and from_version_array agree. For example:
        # MongoDB Enterprise > db.runCommand('buildInfo').versionArray
        # [ 3, 2, 1, -100 ]
        # MongoDB Enterprise > db.runCommand('buildInfo').version
        # 3.2.0-97-g1ef94fe
        if bump_patch_level:
            version[-1] += 1
        version.append(mod)

        return Version(*version)

    @classmethod
    def from_version_array(cls, version_array):
        version = list(version_array)
        if version[-1] < 0:
            version[-1] = -1
        version = cls._padded(version, 3)
        return Version(*version)

    @classmethod
    def from_client(cls, client):
        info = client.server_info()
        if "versionArray" in info:
            return cls.from_version_array(info["versionArray"])
        return cls.from_string(info["version"])

    def at_least(self, *other_version):
        return self >= Version(*other_version)

    def __str__(self):
        return ".".join(map(str, self))
