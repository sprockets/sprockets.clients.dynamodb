How to Contribute
=================
Do you want to contribute fixes or improvements?

   **AWesome!** *Thank you very much, and let's get started.*

Set up a development environment
--------------------------------
The first thing that you need is a development environment so that you can
run the test suite, update the documentation, and everything else that is
involved in contributing.  The easiest way to do that is to create a virtual
environment for your endevours::

   $ python3 -mvenv env

Don't worry about writing code against previous versions of Python unless
you you don't have a choice.  That is why we run our tests through `tox`_.
If you don't have a choice, then install `virtualenv`_ to create the
environment instead.  The next step is to install the development tools
that this project uses.  These are listed in *requires/development.txt*::

   $ env/bin/pip install -qr requires/development.txt

At this point, you will have everything that you need to develop at your
disposal.  *setup.py* is the swiss-army knife in your development tool
chest.  It provides the following commands:

**./setup.py nosetests**
   Run the test suite using `nose`_ and generate a nice coverage report.

**./setup.py build_sphinx**
   Generate the documentation using `sphinx`_.

**./setup.py flake8**
   Run `flake8`_ over the code and report style violations.

If any of the preceding commands give you problems, then you will have to
fix them **before** your pull request will be accepted.

Running Tests
-------------
The easiest (and quickest) way to run the test suite is to use the
*nosetests* command.  It will run the test suite against the currently
installed python version and report not only the test result but the
test coverage as well::

   $ ./setup.py nosetests

   running nosetests
   running egg_info
   writing dependency_links to sprockets.clients.dynamodb.egg-info/dependency_links.txt
   writing top-level names to sprockets.clients.dynamodb.egg-info/top_level.txt
   writing sprockets.clients.dynamodb.egg-info/PKG-INFO
   reading manifest file 'sprockets.clients.dynamodb.egg-info/SOURCES.txt'
   reading manifest template 'MANIFEST.in'
   warning: no previously-included files matching '__pycache__'...
   warning: no previously-included files matching '*.swp' found ...
   writing manifest file 'sprockets.clients.dynamodb.egg-info/SOURCES.txt'
   ...

   Name                       Stmts   Miss Branch BrMiss  Cover   Missing
   ----------------------------------------------------------------------
   ...
   ----------------------------------------------------------------------
   TOTAL                         95      2     59      2    97%
   ----------------------------------------------------------------------
   Ran 44 tests in 0.054s

   OK

That's the quick way to run tests.  The slightly longer way is to run
the `tox`_ utility.  It will run the test suite against all of the
supported python versions in parallel.  This is essentially what Travis-CI
will do when you issue a pull request anyway::

   $ env/bin/tox
   py27 recreate: /.../sprockets.clients.dynamodb/build/tox/py27
   GLOB sdist-make: /.../sprockets.clients.dynamodb/setup.py
   py34 recreate: /.../sprockets.clients.dynamodb/build/tox/py34
   py27 installdeps: -rtest-requirements.txt, mock
   py34 installdeps: -rtest-requirements.txt
   py27 inst: /.../sprockets.clients.dynamodb/build/tox/dist/sprockets.clients.dynamodb-0.0.0.zip
   py27 runtests: PYTHONHASHSEED='2156646470'
   py27 runtests: commands[0] | /../sprockets.clients.dynamodb/build/tox/py27/bin/nosetests
   py34 inst: /../sprockets.clients.dynamodb/.build/tox/dist/sprockets.clients.dynamodb-0.0.0.zip
   py34 runtests: PYTHONHASHSEED='2156646470'
   py34 runtests: commands[0] | /.../sprockets.clients.dynamodb/build/tox/py34/bin/nosetests
   _________________________________ summary _________________________________
     py27: commands succeeded
     py34: commands succeeded
     congratulations :)

This is what you want to see.  Now you can make your modifications and keep
the tests passing.

Submitting a Pull Request
-------------------------
Once you have made your modifications, gotten all of the tests to pass,
and added any necessary documentation, it is time to contribute back for
posterity.  You've probably already cloned this repository and created a
new branch.  If you haven't, then checkout what you have as a branch and
roll back *master* to where you found it.  Then push your repository up
to github and issue a pull request.  Describe your changes in the request,
if Travis isn't too annoyed someone will review it, and eventually merge
it back.

.. _flake8: http://flake8.readthedocs.org/
.. _nose: http://nose.readthedocs.org/
.. _sphinx: http://sphinx-doc.org/
.. _tox: http://testrun.org/tox/
.. _virtualenv: http://virtualenv.pypa.io/
