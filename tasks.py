# -*- coding: utf-8 -*-

from invoke import task

@task
def clean(ctx):
    ctx.run('rm -rf dist')
    ctx.run('rm -rf build')
    ctx.run('rm -rf sqlalchemy_postgres_copy.egg-info')

@task
def publish(ctx, test=False):
    """Publish to the cheeseshop."""
    clean(ctx)
    if test:
        ctx.run('python setup.py register -r test sdist bdist_wheel', echo=True)
        ctx.run('twine upload dist/* -r test', echo=True)
    else:
        ctx.run('python setup.py register sdist bdist_wheel', echo=True)
        ctx.run('twine upload dist/*', echo=True)
