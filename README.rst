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
You can check with

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

To apply changes to the database schema, we use [alembic](https://alembic.sqlalchemy.org/en/latest/index.html).

Alembic is configured to read the information needed to connect to the database 
database from environment variables:

* `DB_NAME`
* `DB_USER`
* `DB_PASSWORD`
* `DB_HOST`, e.g., localhost

To migrate the database to the latest database schema run:

.. code-block:: console
  alembic upgrade

**Important note**: if you are creating the database from scratch, **do not** use 
the alembic database migrations. Instead, use SQLAlchemy to create a complete new 
instance of the database. 

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

  DATABASE_URL=mysql://[user]:[pass]@[host]:[port]/[db_name]

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
