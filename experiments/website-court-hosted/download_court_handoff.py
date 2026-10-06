"""Reuse Bulk's stored-ZIP range extractor for the sealed court handoff only.

Run outside the bridge timer with fsspec==2026.9.0 and aiohttp==3.11.14.
GITHUB_TOKEN must read Actions artifacts in mikewolfd/spicy-regs. Signed URLs
and credentials are never written to evidence. This does not admit the bundle.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import struct
import time
import zipfile
import zlib

import fsspec
import httpx


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def extract_stored_member(stream, info, target, pin):
    """Bulk's proven stored-member seek/read/CRC path, with sealed SHA checks."""
    if info.file_size != pin['bytes'] or info.compress_type != zipfile.ZIP_STORED or info.flag_bits & 1:
        raise ValueError('ZIP member differs from recorded stored, unencrypted member')
    if target.exists():
        if target.stat().st_size != info.file_size or digest(target) != pin['sha256']:
            raise ValueError('Existing selected member differs from sealed bytes')
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + '.extracting')
    offset = temporary.stat().st_size if temporary.exists() else 0
    if not 0 <= offset <= info.file_size:
        raise ValueError('Partial stored member has invalid size')
    crc = 0
    if offset:
        with temporary.open('rb') as prefix:
            while block := prefix.read(8 * 2**20):
                crc = zlib.crc32(block, crc)
    stream.seek(info.header_offset)
    header = stream.read(30)
    if len(header) != 30 or header[:4] != b'PK\x03\x04':
        raise ValueError('Stored ZIP local header is missing')
    name_size, extra_size = struct.unpack('<HH', header[26:30])
    data_offset = info.header_offset + 30 + name_size + extra_size
    stream.seek(data_offset + offset)
    with temporary.open('ab') as outgoing:
        remaining = info.file_size - offset
        while remaining:
            block = stream.read(min(8 * 2**20, remaining))
            if not block:
                raise ValueError('Stored ZIP member is truncated')
            outgoing.write(block)
            crc = zlib.crc32(block, crc)
            remaining -= len(block)
        outgoing.flush()
        os.fsync(outgoing.fileno())
    if (temporary.stat().st_size != info.file_size or crc & 0xffffffff != info.CRC
            or digest(temporary) != pin['sha256']):
        raise ValueError('Stored ZIP CRC or sealed member SHA differs')
    temporary.replace(target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    expected = {
        'court-opinions/build/generation/artifact.json',
        'court-opinions/build/generation/members.json',
        'court-opinions/build/generation/court_opinions.parquet',
        'court-opinions/build/generation/etl_receipts.parquet',
        'court-opinions/conversion.json',
        'court-opinions/captured-publication.v2.json',
    }
    if (manifest['sourceRepository'] != 'mikewolfd/spicy-regs'
            or manifest['sourceRunId'] != 37486836634
            or manifest['artifactId'] != 11434297788
            or {member['name'] for member in manifest['members']} != expected
            or len(manifest['members']) != len(expected)):
        raise ValueError('Only the recorded court handoff may be extracted')
    args.output.mkdir(parents=True, exist_ok=False)
    api = 'https://api.github.com/repos/mikewolfd/spicy-regs/actions/artifacts/11434297788'
    headers = {'Authorization': 'Bearer ' + os.environ['GITHUB_TOKEN'],
               'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28'}
    started = time.monotonic()
    response = httpx.get(api, headers=headers, timeout=30)
    response.raise_for_status()
    artifact = response.json()
    if (artifact['id'] != manifest['artifactId'] or artifact['expired']
            or artifact['size_in_bytes'] != manifest['archiveBytes']
            or artifact['digest'] != 'sha256:' + manifest['archiveSha256']
            or artifact['workflow_run']['id'] != manifest['sourceRunId']):
        raise ValueError('Source artifact metadata differs from the recorded handoff')
    (args.output / 'github-artifact.json').write_text(json.dumps(artifact, indent=2) + '\n')
    response = httpx.get(api + '/zip', headers=headers, follow_redirects=False, timeout=30)
    if response.status_code != 302:
        raise ValueError('Source artifact download requires a readable Actions credential and live redirect')
    # This seekable HTTP reader fetches central-directory/header/member ranges;
    # never pass the URL to a whole-archive downloader or archive.extractall.
    with fsspec.filesystem('http').open(response.headers['location'],
                                      block_size=16 * 2**20, cache_type='bytes') as stream, \
            zipfile.ZipFile(stream) as archive:
        inventory = [{'name': info.filename, 'bytes': info.file_size,
                      'compressedBytes': info.compress_size, 'compression': info.compress_type}
                     for info in archive.infolist()]
        (args.output / 'remote-archive-index.json').write_text(json.dumps(inventory, indent=2) + '\n')
        for member in manifest['members']:
            safe = PurePosixPath(member['name'])
            if safe.is_absolute() or '..' in safe.parts:
                raise ValueError('Selected ZIP member path is unsafe')
            extract_stored_member(stream, archive.getinfo(member['name']),
                                  args.output / 'native-subset' / member['name'], member)
            print(json.dumps({'name': member['name'], 'bytes': member['bytes'],
                              'sha256': member['sha256']}), flush=True)
    report = {'status': 'SEALED_HANDOFF_EXTRACTED_NOT_ADMITTED',
              'sourceRunId': manifest['sourceRunId'], 'artifactId': manifest['artifactId'],
              'selectedBytes': manifest['selectedBytes'], 'archiveBytes': manifest['archiveBytes'],
              'manifestSha256': digest(args.manifest), 'seconds': time.monotonic() - started,
              'diskFreeBytesAfter': shutil.disk_usage(args.output).free,
              'scope': 'Download outside 900-second bridge timer; every bridge check remains required'}
    (args.output / 'extraction.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Network exception text may contain a temporary signed URL.
        raise SystemExit('Court handoff extraction failed: ' + type(error).__name__) from None
