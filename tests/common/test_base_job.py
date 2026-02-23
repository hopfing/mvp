import polars as pl
import pytest

from mvp.common.base_job import BaseJob


class TestBuildPath:
    def test_basic_path(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        path = job.build_path("raw", "tournaments/tour/580/2023")
        expected = (
            tmp_path / "raw" / "atptour" / "tournaments"
            / "tour" / "580" / "2023"
        )
        assert path == expected

    def test_with_filename(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        path = job.build_path(
            "stage", "tournaments/tour/580/2023",
            "results_singles.parquet",
        )
        assert path.name == "results_singles.parquet"

    def test_invalid_bucket_raises(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        with pytest.raises(ValueError, match="Invalid bucket"):
            job.build_path("invalid", "some/path")

    def test_all_valid_buckets(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        for bucket in ("raw", "stage", "analytics"):
            path = job.build_path(bucket, "some/path")
            assert bucket in str(path)


class TestDefaultDataRoot:
    def test_default_data_root_resolves_to_data_dir(self):
        job = BaseJob(domain="atptour")
        # Default should resolve to <project_root>/data
        assert job.data_root.name == "data"
        assert job.data_root.is_absolute()


class TestSaveJson:
    def test_roundtrip(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        data = {"key": "value", "nested": [1, 2, 3]}
        path = tmp_path / "test.json"
        job.save_json(data, path)
        assert job.read_json(path) == data

    def test_creates_parent_dirs(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        path = tmp_path / "a" / "b" / "c" / "test.json"
        job.save_json({"x": 1}, path)
        assert path.exists()
        assert job.read_json(path) == {"x": 1}

    def test_unicode_content(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        data = {"name": "Djokovi\u0107", "city": "Z\u00fcrich"}
        path = tmp_path / "test.json"
        job.save_json(data, path)
        result = job.read_json(path)
        assert result["name"] == "Djokovi\u0107"
        assert result["city"] == "Z\u00fcrich"


class TestSaveParquet:
    def test_saves_with_schema_hash(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        path = tmp_path / "test.parquet"
        result = job.save_parquet(df, path)
        assert result == path
        assert path.exists()

    def test_empty_df_returns_none(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        df = pl.DataFrame({"a": [], "b": []}).cast({"a": pl.Int64, "b": pl.String})
        path = tmp_path / "test.parquet"
        result = job.save_parquet(df, path)
        assert result is None
        assert not path.exists()

    def test_schema_hash_in_metadata(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        path = tmp_path / "test.parquet"
        job.save_parquet(df, path)
        import pyarrow.parquet as pq

        meta = pq.read_metadata(path)
        assert b"schema_hash" in meta.metadata


class TestReadHtml:
    def test_read_html(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        path = tmp_path / "test.html"
        path.write_text("<html><body>Hello</body></html>", encoding="utf-8")
        content = job.read_html(path)
        assert "<html>" in content
        assert "Hello" in content


class TestListFiles:
    def test_list_files(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        (tmp_path / "a.txt").touch()
        (tmp_path / "b.txt").touch()
        (tmp_path / "c.json").touch()
        result = job.list_files(tmp_path, "*.txt")
        assert len(result) == 2
        assert all(p.suffix == ".txt" for p in result)

    def test_list_files_sorted(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        (tmp_path / "c.txt").touch()
        (tmp_path / "a.txt").touch()
        (tmp_path / "b.txt").touch()
        result = job.list_files(tmp_path)
        names = [p.name for p in result]
        assert names == sorted(names)

    def test_list_files_nonexistent_dir(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        result = job.list_files(tmp_path / "nonexistent")
        assert result == []


class TestAtomicWrite:
    def test_no_tmp_file_on_success(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        path = tmp_path / "test.json"
        job.save_json({"a": 1}, path)
        assert not (tmp_path / "test.json.tmp").exists()

    def test_no_tmp_file_on_parquet_success(self, tmp_path):
        job = BaseJob(domain="atptour", data_root=tmp_path)
        df = pl.DataFrame({"a": [1]})
        path = tmp_path / "test.parquet"
        job.save_parquet(df, path)
        assert not (tmp_path / "test.parquet.tmp").exists()
