# -*- coding: utf-8 -*-

from invoke import task, run

@task
def clean():
    run('rm -rf dist')
    run('rm -rf build')
    run('rm -rf sqlalchemy_postgres_copy.egg-info')

@task
def publish(test=False):
    """Publish to the cheeseshop."""
    clean()
    if test:
        run('python setup.py register -r test sdist bdist_wheel', echo=True)
        run('twine upload dist/* -r test', echo=True)
    else:
        run('python setup.py register sdist bdist_wheel', echo=True)
        run('twine upload dist/*', echo=True)
