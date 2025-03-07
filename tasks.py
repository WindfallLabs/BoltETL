"""Build and test tools."""

from subprocess import call

from invoke import task


@task
def test(c):
    call("coverage run -m pytest")
    call("coverage html")
    return


@task
def clean(c):
    call("uvx ruff check --fix")
    call("uvx ruff check --select I --fix .")
    call("uvx ruff format .")
    return
