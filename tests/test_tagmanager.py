import pytest

from pyspark_dnstapir.tags.tag_manager import TagManager, TagManagerHTTPError


def test_tag_manager():
    tm = TagManager(base_url="http://localhost:8080", api_key=None)

    assert tm.get_custom_tags() == []
    assert "anomalous_qtype_distribution" in tm.get_well_defined_tags()

    assert tm.get_domains_with_tag("test_tag") == []
    assert tm.get_tags_on_domain("dnstapir.se") == []

    tm.post_tag("dnstapir.se", "test_tag", "a=1")
    tm.post_tag("dnstapir.se", "test_tag2", None)
    tm.post_tag("example.com", "test_tag", None)

    assert sorted(tm.get_custom_tags()) == ["test_tag", "test_tag2"]
    assert sorted(tm.get_domains_with_tag("test_tag")) == ["dnstapir.se", "example.com"]
    assert tm.get_domains_with_tag("test_tag2") == ["dnstapir.se"]
    assert sorted(tm.get_tags_on_domain("dnstapir.se")) == ["test_tag", "test_tag2"]
    assert tm.get_tags_on_domain("example.com") == ["test_tag"]

    assert tm.get_value("dnstapir.se", "test_tag") == "a=1"
    assert tm.get_value("dnstapir.se", "test_tag2") == ""
    assert tm.get_value("example.com", "test_tag") == ""


def test_tag_manager_404():
    tm = TagManager(base_url="http://localhost:8080", api_key="TEST_KEY")
    with pytest.raises(TagManagerHTTPError):
        tm.get_value("nonexistent-domain.com", "non_existing_tag")
