from unittest.mock import MagicMock

import pandas as pd
import pytest

from harmonize_ds import HARMONIZEDS

mock_df = pd.DataFrame(
    [
        {"class": "Zika", "date": "2017-02-05", "value": 3},
        {"class": "Zika", "date": "2017-02-12", "value": 5},
    ]
)


@pytest.fixture
def mock_harmonizeds(monkeypatch):
    # Mock collections
    monkeypatch.setattr(
        HARMONIZEDS,
        "collections",
        lambda: [
            {"id": "bdc_lcc-wfs", "collection": "bdc_lcc:zika_cases_north_mun_week"},
            {"id": "bdc_lcc-wfs", "collection": "bdc_lcc:dengue_cases_north_mun_week"},
        ],
    )

    class MockCollection:
        title = "Zika Cases North"
        abstract = "Weekly Zika cases in northern municipalities"

        def describe(self):
            return {"title": self.title, "abstract": self.abstract, "classes": ["Zika"]}

        def get(self, filter):
            return mock_df

    monkeypatch.setattr(
        HARMONIZEDS, "get_collection", lambda id, collection_id: MockCollection()
    )

    return HARMONIZEDS


def test_collections(mock_harmonizeds):
    collections = mock_harmonizeds.collections()
    assert isinstance(collections, list)
    assert len(collections) == 2
    assert collections[0]["collection"] == "bdc_lcc:zika_cases_north_mun_week"


def test_get_collection_describe(mock_harmonizeds):
    zica = mock_harmonizeds.get_collection(
        id="bdc_lcc-wfs", collection_id="bdc_lcc:zika_cases_north_mun_week"
    )
    meta = zica.describe()
    assert meta["title"] == "Zika Cases North"
    assert "Zika" in meta["classes"]
    assert zica.title == "Zika Cases North"
    assert zica.abstract == "Weekly Zika cases in northern municipalities"


def test_get_collection_get(mock_harmonizeds):
    zica = mock_harmonizeds.get_collection(
        id="bdc_lcc-wfs", collection_id="bdc_lcc:zika_cases_north_mun_week"
    )
    df = zica.get(
        filter={
            "date": "2017-02-01/2017-02-30",
            "bbox": [-49.15454736, -1.95658217, -48.55769686, -1.69057331],
        }
    )
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 2
    assert "class" in df.columns
