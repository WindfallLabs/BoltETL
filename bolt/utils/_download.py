from io import BytesIO
from pathlib import Path
from urllib.request import urlopen
from zipfile import ZipFile


def memory_unzip(payload) -> list[tuple[str, bytes]]:
    """Unzip the contents of a payload, preserving directory structure."""
    unzipped_payload: list[tuple[str, bytes]] = []
    with ZipFile(BytesIO(payload), "r") as zf:
        # Handle multiple files in zipped payload
        for filename in zf.namelist():
            # Skip directory entries in zip file
            if filename.endswith("/"):
                continue
            # Open the zipped files and extract each
            with zf.open(filename) as file_obj:
                t = (filename, file_obj.read())
                unzipped_payload.append(t)
    return unzipped_payload


def download(url: str, out_dir: Path, unzip=True):
    """Download and extract files, preserving directory structure."""
    # Ensure output directory exists
    out_dir.mkdir(parents=True, exist_ok=True)

    # Get the payload to download
    payload: bytes = urlopen(url).read()

    # Determine if the payload should be unzipped (in-memory)
    if unzip and url.endswith(".zip"):
        # Unzip the payload in-memory
        payload: list[bytes] = memory_unzip(payload)

    if isinstance(payload, list):
        for filename, file_content in payload:
            # Create full path including any subdirectories
            full_path = out_dir / filename
            # Create parent directories if they don't exist
            full_path.parent.mkdir(parents=True, exist_ok=True)
            # Write the file
            with full_path.open("wb") as f:
                f.write(file_content)
    else:
        # Handle non-zip files
        filename = url.split("/")[-1]
        with (out_dir / filename).open("wb") as f:
            f.write(payload)

    return out_dir
