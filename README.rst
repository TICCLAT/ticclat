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

Server security
---------------
Run `sudo mysql_secure_installation` with the following choices:

* Validate passwords: no
* Root password: pick one
* Remove anonymous users: yes
* Disallow root login remotely: no
* Remove test database and access to it: yes
* Reload privilege tables now: yes

To allow login as any user with the root password set above,
you have to switch the authentication plugin for root to
`mysql_native_password`.

.. code-block:: mysql

  SELECT plugin from mysql.user where User='root';

what plugin you are using currently.
If it is auth_socket (default on Ubuntu), you can only login
as root if you are running `mysql` as the Unix root user,
e.g. by running with `sudo`.
To change it to `mysql_native_password`, start `mysql -u root` and run

.. code-block:: mysql

  UPDATE mysql.user SET plugin = 'mysql_native_password' WHERE User = 'root';

To make this authentication plugin the default, add the following
to `/etc/my.cnf` (or another `my.cnf` location, run
`mysqladmin --help` to see the locations that `mysqld` looks for):

.. code-block:: console

  [mysqld]
  default-authentication-plugin = mysql_native_password


Other settings
--------------

To run the ingestion script (e.g. the elex lexicon ingestion),
the maximum package size has to be high enough.
We set it to 41943040 (4194304 was not enough) by setting
the following line in `/etc/my.cnf`:

.. code-block:: console

  [mysqld]
  max_allowed_packet = 42M


To allow for loading CSV files (this is the fastest way of inserting big bulks of records), add:

.. code-block:: console

  [mysqld]
  local_infile=ON

This allows you to run queries like this:

.. code-block:: sql

  LOAD DATA LOCAL INFILE '/file.csv' INTO TABLE test FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\';  

This loads the file `/file.csv` **from the client**, sends it to the server which inserts it into table `test`.
See [MySQL Load Data Documentation](https://dev.mysql.com/doc/refman/8.0/en/load-data.html).

To allow for saving CSV files, add:

.. code-block:: console

  [mysqld]
  secure_file_priv=/data/tmp/mysql

Also, add this to `/etc/apparmor.d/usr.sbin.mysqld` (restart afterwards: `sudo systemctl reload apparmor`)

.. code-block:: console

  # Allow /data/tmp/mysql access
  /data/tmp/mysql/ rw,
  /data/tmp/mysql/** rw,

Make sure the directory /data/tmp/mysql exists and is writable by the `mysql` user.


Ubuntu
------

On Ubuntu 18.04, the default mysqld settings in
`/etc/mysql/mysql.conf.d/mysqld.cnf`
set the socket to a non-standard location that confuses all the default values
in MySQLdb.
Change it to `/tmp/mysql.sock` if you get `OperationError: 2006 ...` when
running `ticclat` tasks like ingesting corpora or lexica.

Changes to the Database Schema
------------------------------

**Important note**: Alembic stripts were removed. Use most recent database dumps
to get the newest version of the database.

To apply changes to the database schema, we use [alembic](https://alembic.sqlalchemy.org/en/latest/index.html).

Alembic is configured to read the information needed to connect to the database
database from environment variable `DATABASE_URL`

To migrate the database to the latest database schema run:

.. code-block:: console

  alembic upgrade head

**Important note**: if you are creating the database from scratch, **do not** use
the alembic database migrations. Instead, use SQLAlchemy to create a complete new
instance of the database.

Data ingestion
**************

The ticclat package contains scripts for ingesting data into the database.
To run the scripts, create an ``.env`` file as described under
`Setup virtual environment`_. In the directory where the `.env` file is located,
type `python` and then:

.. code-block:: console

  >>> from ticclat import ingest
  >>> ingest.run()

You can conigure ``run()`` by providing arguments:

* ``env``: path to the ``.env`` file (default: ``.env``)
* ``reset_db``: delete the database and recreate it before ingesting data (default: ``False``)
* ``alphabet_file``: path to the alphabet file (required for calculating anahashes;
  default: `/data/ALPH/nld.aspell.dict.clip20.lc.LD3.charconfus.clip20.lc.chars`)
* ``batch_size``: size of database batches (default: 5000) (We should look into how this is used.)
* ``include``: list of data sources to ingest (default: ``[]``)
* ``exclude``: list of data sources to exclude from ingesting (default: ``[]``)
* ``ingest``: boolean indicating whether data should be ingested (default: ``True``)
* ``anahash``: boolean indicating whether anahashes should be calculated (default: ``True``)
* ``tmpdir``: directory to use for storing temporary data (default: ``/data/tmp``)
* ``loglevel``: what log messages to show (default: ``INFO``)
* ``reset_anahashes`` boolean indicating whether the anahashes table should be
  emptied (default: ``False``)
* ``base_dir``: path to the directory containing the source datafiles

The following sources can be ingested (and added to the ``include`` and ``exclude`` lists):

* ``twente``: spelling correction lexicon
* ``inl``: lexicon
* ``SoNaR500``: corpus
* ``elex``: lexicon
* ``groene boekje``: lexicon
* ``OpenTaal``: lexicon
* ``sgd``: Staten Generaal Digitaal, corpus
* ``edbo``: Early Dutch Books Online, corpus
* ``dbnl``: Digitale Bibliotheek voor de Nederlandse letteren
* ``morph_par``: Morphological Paradigms
* ``wf_freqs``: Generate materialized view (table) containing wordforms and their
  total frequencies in the corpora
* ``sgd_ticcl``: ingest ticcl corrections based on the SDG data (we currently have
  data for two wordforms: *Amsterdam* and *Binnenlandsche*)

Flask web app
*************

Preparation
-----------

Starting from Ubuntu (18.04), setup the MySQL database. Then clone this directory, install dependencies (`conda` & `libmysqlclient-dev` & `build-essential` e.g. https://docs.conda.io/en/latest/miniconda.html and `apt-get update && apt-get install -y libmysqlclient-dev build-essential`).

Setup virtual environment
-------------------------

.. code-block:: console

  conda create --name ticclat-web
  conda activate ticclat-web
  conda install pip

From ticclat directory, install it:

.. code-block:: console
  pip install -e .


Create a `.env` file with the following:

.. code-block:: console

  DATABASE_URL=mysql://[user]:[pass]@[host]:[port]/[db_name]?charset=utf8mb4&local_infile=1

  FLASK_APP=ticclat.flask_app.py
  FLASK_ENV=production
  FLASK_DEBUG=0

  #for DEV:
  #FLASK_ENV=development
  #FLASK_DEBUG=1

You can now run a development server using: `flask run`

Or a production server:

.. code-block:: console

  export $(cat .env | xargs)
  gunicorn ticclat.flask_app:app --bind 0.0.0.0:8000

Debugger
********
If the debugger in e.g. PyCharm isn't working correctly, this might be because test coverage is enabled.
Disbable this temporarily by commenting `addopts` line in `setup.cfg`:

.. code-block::

  [tool:pytest]
  testpaths = tests
  #addopts = --cov --cov-report xml --cov-report term --cov-report html


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
