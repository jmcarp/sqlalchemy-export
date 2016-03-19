========================
sqlalchemy-postgres-copy
========================

.. image:: https://img.shields.io/pypi/v/sqlalchemy-postgres-copy.svg
    :target: http://badge.fury.io/py/sqlalchemy-postgres-copy
    :alt: Latest version

.. image:: https://img.shields.io/travis/jmcarp/sqlalchemy-postgres-copy.svg
    :target: https://travis-ci.org/jmcarp/sqlalchemy-postgres-copy
    :alt: Travis-CI

**sqlalchemy-postgres-copy** is a utility library that wraps the PostgreSQL COPY_ command for use with SQLAlchemy. The COPY command offers performant exports from PostgreSQL to TSV, CSV, or binary files, as well as imports from files to PostgresSQL tables. Using COPY is typically much more efficient than importing and exporting data using Python.

Installation
============

::

    pip install -U sqlalchemy-postgres-copy

Usage
=====

::

    import postgres_copy
    from models import Album, session, engine

    # Export a CSV containing all Queen albums
    query = session.query(Album).filter_by(artist='Queen')
    with open('/path/to/file.csv', 'w') as fp:
        postgres_copy.copy_from(query, fp, engine, format='csv', header=True)

    # Import a tab-delimited file
    with open('/path/to/file.tsv') as fp:
        postgres_copy.copy_to(fp, Album, engine)

.. _COPY: http://www.postgresql.org/docs/9.5/static/sql-copy.html
