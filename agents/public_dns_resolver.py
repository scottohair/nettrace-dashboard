#!/usr/bin/env python3
"""Public DNS UDP fallback resolver.

Used when local/system resolver returns no addresses for critical venue hosts.
This module intentionally has zero third-party dependencies.
"""

from __future__ import annotations

import secrets
import socket
import struct
from typing import Iterable

DEFAULT_PUBLIC_RESOLVERS = ("1.1.1.1", "8.8.8.8", "9.9.9.9")


def parse_resolver_list(value: str | Iterable[str] | None, default=None):
    if default is None:
        default = DEFAULT_PUBLIC_RESOLVERS
    if isinstance(value, str):
        items = value.split(",")
    elif isinstance(value, Iterable):
        items = list(value)
    else:
        items = []
    out = []
    seen = set()
    for item in items:
        ip = str(item or "").strip()
        if not ip or ip in seen:
            continue
        seen.add(ip)
        out.append(ip)
    if out:
        return tuple(out)
    return tuple(parse_resolver_list(default, ()))


def _encode_qname(host: str):
    labels = [label for label in str(host or "").strip(".").split(".") if label]
    encoded = bytearray()
    for label in labels:
        raw = label.encode("idna")
        if not raw or len(raw) > 63:
            return b""
        encoded.append(len(raw))
        encoded.extend(raw)
    encoded.append(0)
    return bytes(encoded)


def _skip_name(packet: bytes, offset: int):
    jumped = False
    seen = 0
    while True:
        if offset >= len(packet):
            return None
        length = packet[offset]
        if length == 0:
            offset += 1
            return offset
        # compression pointer
        if (length & 0xC0) == 0xC0:
            if offset + 1 >= len(packet):
                return None
            if not jumped:
                offset += 2
                jumped = True
            return offset
        offset += 1 + int(length)
        seen += 1
        if seen > 255:
            return None


def _parse_dns_answers(packet: bytes, txid: int, qtype: int):
    if len(packet) < 12:
        return []
    rid, flags, qdcount, ancount, _nscount, _arcount = struct.unpack("!HHHHHH", packet[:12])
    if rid != int(txid):
        return []
    # response flag required
    if (flags & 0x8000) == 0:
        return []
    # rcode must be NOERROR
    if (flags & 0x000F) != 0:
        return []
    offset = 12
    for _ in range(int(qdcount)):
        offset = _skip_name(packet, offset)
        if offset is None or (offset + 4) > len(packet):
            return []
        offset += 4

    ips = []
    for _ in range(int(ancount)):
        offset = _skip_name(packet, offset)
        if offset is None or (offset + 10) > len(packet):
            break
        rr_type, rr_class, _ttl, rdlength = struct.unpack("!HHIH", packet[offset:offset + 10])
        offset += 10
        if offset + int(rdlength) > len(packet):
            break
        rdata = packet[offset:offset + int(rdlength)]
        offset += int(rdlength)
        if int(rr_class) != 1:
            continue
        if int(rr_type) == 1 and int(rdlength) == 4 and int(qtype) == 1:
            try:
                ips.append(socket.inet_ntoa(rdata))
            except Exception:
                continue
        elif int(rr_type) == 28 and int(rdlength) == 16 and int(qtype) == 28:
            try:
                ips.append(socket.inet_ntop(socket.AF_INET6, rdata))
            except Exception:
                continue
    deduped = []
    seen = set()
    for ip in ips:
        if ip in seen:
            continue
        seen.add(ip)
        deduped.append(ip)
    return deduped


def _query_dns_udp(host: str, resolver_ip: str, qtype: int, timeout_seconds: float):
    qname = _encode_qname(host)
    if not qname:
        return []
    txid = secrets.randbelow(65535)
    header = struct.pack("!HHHHHH", txid, 0x0100, 1, 0, 0, 0)  # RD=1
    question = qname + struct.pack("!HH", int(qtype), 1)
    packet = header + question
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(max(0.05, float(timeout_seconds)))
        sock.sendto(packet, (str(resolver_ip), 53))
        resp, _addr = sock.recvfrom(4096)
        return _parse_dns_answers(resp, txid=txid, qtype=qtype)
    except Exception:
        return []
    finally:
        try:
            sock.close()
        except Exception:
            pass


def resolve_host_via_public_dns(
    host: str,
    resolvers=None,
    timeout_seconds: float = 1.0,
    max_ips: int = 8,
    include_ipv6: bool = False,
):
    host_s = str(host or "").strip().lower()
    if not host_s:
        return []
    resolver_list = parse_resolver_list(resolvers, DEFAULT_PUBLIC_RESOLVERS)
    if not resolver_list:
        return []

    qtypes = [1]  # A
    if include_ipv6:
        qtypes.append(28)  # AAAA

    out = []
    seen = set()
    for resolver in resolver_list:
        for qtype in qtypes:
            ips = _query_dns_udp(
                host=host_s,
                resolver_ip=str(resolver),
                qtype=int(qtype),
                timeout_seconds=float(timeout_seconds),
            )
            for ip in ips:
                ip_s = str(ip).strip()
                if not ip_s or ip_s in seen:
                    continue
                seen.add(ip_s)
                out.append(ip_s)
                if len(out) >= max(1, int(max_ips)):
                    return out
        if out:
            return out
    return out
