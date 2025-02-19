"""Build and test tools."""

from subprocess import call

from invoke import task


@task
def test(c):
    call("coverage run -m pytest")
    call("coverage html")
    return
