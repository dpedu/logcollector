#!/bin/sh -ex

dpkg-buildpackage -us -uc -b

mv ../irclogtools_1.0.0-1_a* ./
rm *.changes
