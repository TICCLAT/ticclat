################################################################################
ticclat
################################################################################

TICCLAT is a tool for text-induced corpus correction and lexical assessment.

.. image:: https://travis-ci.org/TICCLAT/ticclat.svg?branch=master
    :target: https://travis-ci.org/TICCLAT/ticclat

Installation
************

To install ticclat, do:

.. code-block:: console

  git clone https://github.com/ticclat/ticclat.git
  cd ticclat
  pip install .


Run tests (including coverage) with:

.. code-block:: console

  python setup.py test


Setup MySQL
***********

Configure server
----------------
Run `sudo mysql_secure_installation`_ with the following choices:

* Validate passwords: no
* Root password: pick one
* Remove anonymous users: yes
* Disallow root login remotely: no
* Remove test database and access to it: yes
* Reload privilege tables now: yes

To run the ingestion script (e.g. the elex lexicon ingestion), the maximum package size has to be high enough.
We set it to 41943040 (4194304 was not enough) by setting the following line in `/etc/my.cnf`_:

.. code-block:: console

  [mysqld]
  max_allowed_packet = 42M


Documentation
*************

.. _README:

Include a link to your project's full documentation here.

Contributing
************

If you want to contribute to the development of ticclat,
have a look at the `contribution guidelines <CONTRIBUTING.rst>`_.

License
*******

Copyright (c) 2019, Netherlands eScience Center and Meertens Instituut

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.



Credits
*******

This package was created with `Cookiecutter <https://github.com/audreyr/cookiecutter>`_ and the `NLeSC/python-template <https://github.com/NLeSC/python-template>`_.
