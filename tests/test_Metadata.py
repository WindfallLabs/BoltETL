from bolt import Metadata


def test_metadata():
    m = Metadata(description="This is test metadata", tags=set(["test", "metadata"]))

    assert "#test" in m.tags
    assert m.description
    assert m.datasource is None
    assert repr(m) == "<Metadata(datasource=None)>"


def test_metadata_kwargs():
    m = Metadata(kwargs={"my_test_kwargs": True, "what_the_kids_say": "yeet"})

    assert m.kwargs == {"my_test_kwargs": True, "what_the_kids_say": "yeet"}
    assert m.what_the_kids_say == "yeet"
