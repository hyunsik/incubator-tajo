************************************************
Dowload the latest source release of Apache Tajo
************************************************

========================================
How to Verify the integrity of the files
========================================

It is essential that you verify the integrity of the downloaded files using the PGP or MD5 signatures. Please read Verifying Apache HTTP Server Releases for more information on why you should verify our releases.

The PGP signatures can be verified using PGP or GPG. First download the `KEYS <http://www.apache.org/dist/incubator/tajo/KEYS>`_ as well as the asc signature file for the relevant distribution. Make sure you get these files from `the main distribution directory <http://www.apache.org/dist/incubator/tajo/>`_, rather than from a mirror. Then verify the signatures using  ::

  % pgpk -a KEYS

  % pgpv tajo-0.X.0-incubating-src.tar.gz.asc

or ::

  % pgp -ka KEYS

  % pgp tajo-0.X.0-incubating-src.tar.gz.asc

or ::

  % gpg --import KEYS

  % gpg --verify tajo-0.X.0-incubating-src.tar.gz.asc

=======================================
How to verify SHA1/MD5 hash values
=======================================

Alternatively, you can verify the MD5 signature on the files. A unix program called md5 or md5sum is included in many unix distributions. It is also available as part of 
`GNU Textutils <http://www.gnu.org/software/textutils/textutils.html>`_. An MD5 signature consists of 32 hex characters, and a SHA1 signature consists of 40 hex characters. You can verify the signatures as follows: ::

	md5sum -c tajo-0.x.0-incubating-src.tar.gz.md5

or ::

	sha1sum -c tajo-0.x.0-incubating-src.tar.gz.sha1