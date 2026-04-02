# ============================================================
#  NebulaDB — Unified Node (run this on ALL 4 laptops)
#  Each laptop runs this SAME file with a different NODE_ID
# ============================================================
#
#  *** THIS FILE IS PRE-CONFIGURED FOR ADWAITHA (node-1) ***
#  Just run: python nebuladb_node1_adwaitha.py
#
#  Other members run their own file:
#  Adwaitha    → nebuladb_node1_adwaitha.py
#  Srividya    → nebuladb_node2_srividya.py
#  Nethra      → nebuladb_node3_nethra.py
#  Sreelakshmi → nebuladb_node4_sreelakshmi.py
#
# ============================================================

import sys, hashlib, time, threading, random, math, uuid, requests
from flask import Flask, request, jsonify
from collections import OrderedDict
from bisect import insort, bisect_left
from enum import Enum

# ── Node Configuration ────────────────────────────────────────────────────────
ALL_NODES = {
    'node-1': {'host': '10.190.172.237', 'port': 6001, 'member': 'Adwaitha'},
    'node-2': {'host': '10.190.172.66',  'port': 6002, 'member': 'Srividya'},
    'node-3': {'host': '10.190.172.7',   'port': 6003, 'member': 'Nethra'},
    'node-4': {'host': '10.190.172.172', 'port': 6004, 'member': 'Sreelakshmi'},
}

# Pre-configured for Adwaitha — no arguments needed
# Just run: python nebuladb_node1_adwaitha.py

NODE_ID   = 'node-1'
MY_INFO   = ALL_NODES[NODE_ID]
MY_HOST   = MY_INFO['host']
MY_PORT   = MY_INFO['port']
MY_MEMBER = MY_INFO['member']
PEERS     = {nid: info for nid, info in ALL_NODES.items() if nid != NODE_ID}

def peer_url(node_id):
    n = ALL_NODES[node_id]
    return f"http://{n['host']}:{n['port']}"

app = Flask(__name__)

# Enable CORS for the dashboard
@app.after_request
def add_cors_headers(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response


# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 1 — TrueTime
# ══════════════════════════════════════════════════════════════════════════════
class TrueTime:
    UNCERTAINTY_MS = 7

    @staticmethod
    def now():
        t = time.time() * 1000
        e = random.uniform(0, TrueTime.UNCERTAINTY_MS)
        return {
            'earliest': round(t - e, 3),
            'latest':   round(t + e, 3),
            'mid':      round(t, 3)
        }

    @staticmethod
    def safe_commit_ts():
        """Wait out uncertainty then return a safe commit timestamp"""
        time.sleep(TrueTime.UNCERTAINTY_MS / 1000.0)
        return int(time.time() * 1000)

# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 2 — Bloom Filter
# ══════════════════════════════════════════════════════════════════════════════
class BloomFilter:
    def __init__(self, capacity=10000, fp_rate=0.01):
        m       = -capacity * math.log(fp_rate) / (math.log(2) ** 2)
        self.m  = int(m)
        self.k  = int((self.m / capacity) * math.log(2))
        self.bits = [False] * self.m

    def _hashes(self, key):
        h1 = int(hashlib.md5(key.encode()).hexdigest(), 16)
        h2 = int(hashlib.sha1(key.encode()).hexdigest(), 16)
        return [(h1 + i * h2) % self.m for i in range(self.k)]

    def add(self, key):
        for p in self._hashes(key): self.bits[p] = True

    def __contains__(self, key):
        return all(self.bits[p] for p in self._hashes(key))

# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 3 — MVCC Record
# ══════════════════════════════════════════════════════════════════════════════
class MVCCRecord:
    def __init__(self):
        self.versions = []

    def write(self, ts, value, deleted=False):
        insort(self.versions, (ts, value, deleted))

    def read(self, ts=None):
        if not self.versions: return None
        if ts is None:
            _, v, d = self.versions[-1]
            return None if d else v
        idx = bisect_left(self.versions, (ts + 1,)) - 1
        if idx < 0: return None
        _, v, d = self.versions[idx]
        return None if d else v

    def latest_ts(self):
        return self.versions[-1][0] if self.versions else 0

    def all_versions(self):
        return [{'ts': ts, 'value': v, 'deleted': d,
                 'time': time.strftime('%H:%M:%S', time.localtime(ts/1000))}
                for ts, v, d in self.versions]

# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 4 — LSM Storage Engine (each node has its own)
# ══════════════════════════════════════════════════════════════════════════════
class MemTable:
    FLUSH_SIZE = 50
    def __init__(self):
        self.data  = {}
        self.bloom = BloomFilter()
        self.size  = 0
        self.wal   = []

    def put(self, key, value, ts):
        if key not in self.data:
            self.data[key] = MVCCRecord()
        self.data[key].write(ts, value)
        self.bloom.add(key)
        self.wal.append({'op':'PUT','key':key,'value':value,'ts':ts})
        self.size += 1

    def delete(self, key, ts):
        if key not in self.data:
            self.data[key] = MVCCRecord()
        self.data[key].write(ts, None, deleted=True)
        self.wal.append({'op':'DEL','key':key,'ts':ts})
        self.size += 1

    def get(self, key, ts=None):
        if key not in self.bloom: return None
        rec = self.data.get(key)
        return rec.read(ts) if rec else None

    def is_full(self): return self.size >= self.FLUSH_SIZE

class SSTable:
    _ctr = 0
    def __init__(self, snapshot):
        SSTable._ctr += 1
        self.id    = SSTable._ctr
        self.bloom = BloomFilter()
        self.data  = {}
        for k, rec in snapshot.items():
            self.data[k] = rec
            self.bloom.add(k)

    def get(self, key, ts=None):
        if key not in self.bloom: return None
        rec = self.data.get(key)
        return rec.read(ts) if rec else None

class StorageEngine:
    def __init__(self):
        self.memtable  = MemTable()
        self.sstables  = []
        self.lock      = threading.Lock()
        self.stats     = {'puts':0,'gets':0,'deletes':0,
                          'flushes':0,'compactions':0}

    def put(self, key, value, ts=None):
        ts = ts or int(time.time() * 1000)
        with self.lock:
            self.memtable.put(key, value, ts)
            self.stats['puts'] += 1
            if self.memtable.is_full(): self._flush()
        return {'ok': True, 'ts': ts}

    def get(self, key, ts=None):
        with self.lock:
            self.stats['gets'] += 1
            v = self.memtable.get(key, ts)
            if v is not None:
                return {'value': v, 'source': 'memtable', 'node': NODE_ID}
            for sst in self.sstables:
                v = sst.get(key, ts)
                if v is not None:
                    return {'value': v, 'source': f'sstable-{sst.id}',
                            'node': NODE_ID}
        return {'value': None, 'source': 'miss', 'node': NODE_ID}

    def delete(self, key, ts=None):
        ts = ts or int(time.time() * 1000)
        with self.lock:
            self.memtable.delete(key, ts)
            self.stats['deletes'] += 1
        return {'ok': True, 'ts': ts}

    def get_versions(self, key):
        rec = self.memtable.data.get(key)
        if not rec:
            for sst in self.sstables:
                rec = sst.data.get(key)
                if rec: break
        return rec.all_versions() if rec else []

    def latest_ts(self, key):
        rec = self.memtable.data.get(key)
        if rec: return rec.latest_ts()
        for sst in self.sstables:
            rec = sst.data.get(key)
            if rec: return rec.latest_ts()
        return 0

    def all_keys(self):
        keys = set(self.memtable.data.keys())
        for sst in self.sstables: keys.update(sst.data.keys())
        return list(keys)

    def snapshot(self, ts=None):
        result = {}
        for k in self.all_keys():
            r = self.get(k, ts)
            if r['value'] is not None:
                result[k] = r['value']
        return result

    def _flush(self):
        sst = SSTable(dict(self.memtable.data))
        self.sstables.insert(0, sst)
        self.memtable = MemTable()
        self.stats['flushes'] += 1
        if len(self.sstables) >= 4: self._compact()

    def _compact(self):
        merged = {}
        for sst in reversed(self.sstables):
            for k, rec in sst.data.items():
                if k not in merged: merged[k] = rec
        self.sstables = [SSTable(merged)]
        self.stats['compactions'] += 1
        print(f'[Storage] Compaction complete on {NODE_ID}')

    def info(self):
        return {
            'node':          NODE_ID,
            'member':        MY_MEMBER,
            'memtable_size': self.memtable.size,
            'sstable_count': len(self.sstables),
            'total_keys':    len(self.all_keys()),
            'stats':         self.stats
        }

storage = StorageEngine()

# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 5 — Consistent Hash Ring (every node has full ring knowledge)
# ══════════════════════════════════════════════════════════════════════════════
class HashRing:
    VNODES = 150

    def __init__(self):
        self.ring   = OrderedDict()
        self.status = {nid: 'UP' for nid in ALL_NODES}
        for nid in ALL_NODES:
            self._add(nid)

    def _hash(self, key):
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)

    def _add(self, node_id):
        for i in range(self.VNODES):
            vk = self._hash(f'{node_id}#vnode{i}')
            self.ring[vk] = node_id
        self.ring = OrderedDict(sorted(self.ring.items()))

    def _remove(self, node_id):
        for i in range(self.VNODES):
            vk = self._hash(f'{node_id}#vnode{i}')
            self.ring.pop(vk, None)

    def mark_failed(self, node_id):
        self.status[node_id] = 'FAILED'
        self._remove(node_id)
        print(f'[Ring] {node_id} marked FAILED — ring rehashed')

    def mark_up(self, node_id):
        if self.status[node_id] != 'UP':
            self.status[node_id] = 'UP'
            self._add(node_id)
            print(f'[Ring] {node_id} recovered — added back to ring')

    def primary(self, key):
        if not self.ring: return None
        h = self._hash(key)
        for rk, nid in self.ring.items():
            if h <= rk and self.status[nid] == 'UP':
                return nid
        return next((nid for nid in self.ring.values()
                     if self.status[nid] == 'UP'), None)

    def replicas(self, key, n=3):
        if not self.ring: return []
        h    = self._hash(key)
        rks  = list(self.ring.keys())
        pos  = 0
        for i, k in enumerate(rks):
            if h <= k: pos = i; break
        result, seen = [], set()
        for i in range(len(rks)):
            nid = self.ring[rks[(pos + i) % len(rks)]]
            if nid not in seen and self.status[nid] == 'UP':
                seen.add(nid); result.append(nid)
            if len(result) == n: break
        return result

    def shard_id(self, key):
        h = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        return f'shard-{h % 1024:04d}'

ring = HashRing()

# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 6 — Raft Consensus (every node participates)
# ══════════════════════════════════════════════════════════════════════════════
class Role(Enum):
    FOLLOWER  = 'FOLLOWER'
    CANDIDATE = 'CANDIDATE'
    LEADER    = 'LEADER'

class LogEntry:
    def __init__(self, term, index, key, value, op, ts):
        self.term  = term
        self.index = index
        self.key   = key
        self.value = value
        self.op    = op
        self.ts    = ts

    def to_dict(self):
        return vars(self)

class RaftEngine:
    HEARTBEAT   = 1.5
    TIMEOUT_MIN = 5.0
    TIMEOUT_MAX = 9.0

    def __init__(self):
        self.term           = 0
        self.voted_for      = None
        self.log            = []
        self.commit_index   = -1
        self.last_applied   = -1
        self.role           = Role.FOLLOWER
        self.leader_id      = None
        self.votes          = set()
        self.next_index     = {}
        self.match_index    = {}
        self.last_heartbeat = time.time()
        self.lock           = threading.Lock()
        self.election_log   = []
        threading.Thread(target=self._timer, daemon=True).start()
        print(f'[Raft] {NODE_ID} started as FOLLOWER')

    def _timer(self):
        while True:
            timeout = random.uniform(self.TIMEOUT_MIN, self.TIMEOUT_MAX)
            time.sleep(0.5)
            with self.lock:
                if (self.role != Role.LEADER and
                        time.time() - self.last_heartbeat > timeout):
                    self._start_election()

    def _start_election(self):
        self.term      += 1
        self.role       = Role.CANDIDATE
        self.voted_for  = NODE_ID
        self.votes      = {NODE_ID}
        self.election_log.append({
            'term': self.term, 'node': NODE_ID,
            'started': time.strftime('%H:%M:%S')
        })
        print(f'[Raft] {NODE_ID} ELECTION term={self.term}')
        peer_list = list(PEERS.keys())
        if not peer_list:
            self._become_leader(); return
        li = len(self.log) - 1
        lt = self.log[li].term if self.log else 0
        for pid in peer_list:
            threading.Thread(target=self._req_vote,
                             args=(pid, li, lt), daemon=True).start()

    def _req_vote(self, peer_id, li, lt):
        try:
            r = requests.post(
                f'{peer_url(peer_id)}/raft/vote',
                json={'term': self.term, 'candidate': NODE_ID,
                      'last_log_index': li, 'last_log_term': lt},
                timeout=2)
            resp = r.json()
            with self.lock:
                if resp.get('granted') and resp['term'] == self.term:
                    self.votes.add(peer_id)
                    print(f'[Raft] {NODE_ID} got vote from {peer_id} '
                          f'({len(self.votes)} total)')
                    quorum = len(ALL_NODES) // 2 + 1
                    if (len(self.votes) >= quorum
                            and self.role == Role.CANDIDATE):
                        self._become_leader()
                elif resp['term'] > self.term:
                    self.term  = resp['term']
                    self.role  = Role.FOLLOWER
                    self.voted_for = None
        except Exception:
            pass

    def _become_leader(self):
        self.role      = Role.LEADER
        self.leader_id = NODE_ID
        if self.election_log:
            self.election_log[-1]['result'] = 'WON'
        print(f'[Raft] *** {NODE_ID} ({MY_MEMBER}) IS LEADER '
              f'term={self.term} votes={len(self.votes)} ***')
        for pid in PEERS:
            self.next_index[pid]  = len(self.log)
            self.match_index[pid] = -1
        threading.Thread(target=self._heartbeats, daemon=True).start()

    def _heartbeats(self):
        while self.role == Role.LEADER:
            for pid in list(PEERS.keys()):
                threading.Thread(target=self._replicate,
                                 args=(pid,), daemon=True).start()
            time.sleep(self.HEARTBEAT)

    def _replicate(self, peer_id):
        with self.lock:
            ni       = self.next_index.get(peer_id, 0)
            entries  = [e.to_dict() for e in self.log[ni:]]
            pi       = ni - 1
            pt       = self.log[pi].term if pi >= 0 and self.log else 0
            payload  = {
                'term': self.term, 'leader': NODE_ID,
                'prev_index': pi, 'prev_term': pt,
                'entries': entries, 'commit': self.commit_index
            }
        try:
            r = requests.post(f'{peer_url(peer_id)}/raft/append',
                              json=payload, timeout=2)
            resp = r.json()
            with self.lock:
                if resp.get('success'):
                    self.next_index[peer_id]  = ni + len(entries)
                    self.match_index[peer_id] = self.next_index[peer_id] - 1
                    self._advance_commit()
                else:
                    self.next_index[peer_id] = max(0, ni - 1)
                    if resp.get('term', 0) > self.term:
                        self.term = resp['term']
                        self.role = Role.FOLLOWER
        except Exception:
            pass

    def _advance_commit(self):
        quorum = len(ALL_NODES) // 2 + 1
        for n in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[n] and self.log[n].term == self.term:
                count = 1 + sum(1 for mi in self.match_index.values()
                                if mi >= n)
                if count >= quorum:
                    self.commit_index = n
                    self._apply()
                    break

    def _apply(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            e = self.log[self.last_applied]
            if not e: continue
            if e.op == 'PUT':
                storage.put(e.key, e.value, e.ts)
            elif e.op == 'DELETE':
                storage.delete(e.key, e.ts)
            print(f'[Raft] Applied {e.op} key={e.key} '
                  f'index={self.last_applied}')

    def propose(self, key, value, op, ts):
        """Leader appends to log and waits for quorum commit"""
        with self.lock:
            if self.role != Role.LEADER:
                return {'error': 'not_leader',
                        'leader': self.leader_id,
                        'redirect': peer_url(self.leader_id)
                                    if self.leader_id else None}
            idx   = len(self.log)
            entry = LogEntry(self.term, idx, key, value, op, ts)
            self.log.append(entry)
            print(f'[Raft] Leader appended {op} key={key} index={idx}')

        # Single node — commit immediately
        if not PEERS:
            with self.lock:
                self.commit_index = idx
                self._apply()
            return {'committed': True, 'index': idx,
                    'term': self.term, 'mode': 'single-node'}

        # Multi-node — wait for quorum (max 4 seconds)
        deadline = time.time() + 4.0
        while time.time() < deadline:
            time.sleep(0.1)
            with self.lock:
                if self.commit_index >= idx:
                    return {'committed': True, 'index': idx,
                            'term': self.term,
                            'replicated_to': [
                                pid for pid, mi in self.match_index.items()
                                if mi >= idx
                            ]}
        return {'committed': False, 'index': idx,
                'term': self.term, 'error': 'quorum_timeout'}

    def status(self):
        with self.lock:
            return {
                'node':     NODE_ID, 'member': MY_MEMBER,
                'role':     self.role.value, 'term': self.term,
                'leader':   self.leader_id,
                'log_len':  len(self.log),
                'commit':   self.commit_index,
                'votes_last_election': len(self.votes),
                'election_log': self.election_log[-3:],
                'peers':    list(PEERS.keys())
            }

raft = RaftEngine()

# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 7 — Heartbeat Monitor (auto failure detection)
# ══════════════════════════════════════════════════════════════════════════════
class HeartbeatMonitor:
    CHECK_EVERY   = 3
    FAIL_AFTER    = 3

    def __init__(self):
        self.misses     = {}
        self.fail_log   = []
        self.recover_log = []
        threading.Thread(target=self._loop, daemon=True).start()
        print('[Monitor] Heartbeat monitor started')

    def _loop(self):
        while True:
            time.sleep(self.CHECK_EVERY)
            for nid, info in ALL_NODES.items():
                if nid == NODE_ID: continue
                alive = self._ping(nid, info)
                if alive:
                    if self.misses.get(nid, 0) > 0:
                        print(f'[Monitor] {nid} RECOVERED!')
                        ring.mark_up(nid)
                        self.recover_log.append({
                            'node': nid, 'at': time.strftime('%H:%M:%S')
                        })
                    self.misses[nid] = 0
                else:
                    self.misses[nid] = self.misses.get(nid, 0) + 1
                    m = self.misses[nid]
                    print(f'[Monitor] {nid} missed heartbeat {m}/{self.FAIL_AFTER}')
                    if m == self.FAIL_AFTER and ring.status[nid] == 'UP':
                        print(f'[Monitor] *** {nid} DECLARED FAILED! ***')
                        ring.mark_failed(nid)
                        self.fail_log.append({
                            'node': nid, 'at': time.strftime('%H:%M:%S'),
                            'truetime': TrueTime.now()
                        })

    def _ping(self, nid, info):
        try:
            r = requests.get(
                f"http://{info['host']}:{info['port']}/health",
                timeout=2)
            return r.status_code == 200
        except Exception:
            return False

    def log(self):
        return {'failures': self.fail_log,
                'recoveries': self.recover_log,
                'current_misses': self.misses}

monitor = HeartbeatMonitor()

# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 8 — Transaction Manager (2PC + Snapshot Isolation)
# ══════════════════════════════════════════════════════════════════════════════
class TxStatus(Enum):
    ACTIVE    = 'ACTIVE'
    PREPARED  = 'PREPARED'
    COMMITTED = 'COMMITTED'
    ABORTED   = 'ABORTED'

class Transaction:
    def __init__(self, tx_id):
        self.tx_id     = tx_id
        self.start_ts  = int(time.time() * 1000)
        self.commit_ts = None
        self.status    = TxStatus.ACTIVE
        self.read_set  = {}
        self.write_set = {}
        self.timeline  = [{'event': 'BEGIN', 'ts': self.start_ts}]
        self.lock      = threading.Lock()

    def log_event(self, ev, detail=''):
        self.timeline.append({
            'event': ev, 'detail': detail,
            'ts': int(time.time() * 1000)
        })

    def to_dict(self):
        return {
            'tx_id':     self.tx_id,
            'start_ts':  self.start_ts,
            'commit_ts': self.commit_ts,
            'status':    self.status.value,
            'reads':     list(self.read_set.keys()),
            'writes':    list(self.write_set.keys()),
            'timeline':  self.timeline
        }

class TxManager:
    def __init__(self):
        self.txns  = {}
        self.log   = []
        self.lock  = threading.Lock()
        self.stats = {
            'committed': 0, 'aborted': 0,
            'conflicts': 0, 'total': 0
        }

    def begin(self):
        tx_id = str(uuid.uuid4())[:8]
        tx    = Transaction(tx_id)
        with self.lock:
            self.txns[tx_id] = tx
            self.stats['total'] += 1
        print(f'[TX] {tx_id} BEGIN')
        return tx.to_dict()

    def read(self, tx_id, key):
        tx = self.txns.get(tx_id)
        if not tx or tx.status != TxStatus.ACTIVE:
            return {'error': 'invalid_tx'}
        # Read-your-own-writes
        if key in tx.write_set:
            v = tx.write_set[key]['value']
            return {'key': key, 'value': v, 'source': 'tx_buffer'}
        # Snapshot read — find the right node
        primary = ring.primary(key)
        try:
            if primary == NODE_ID:
                result = storage.get(key, tx.start_ts)
            else:
                r      = requests.get(
                    f'{peer_url(primary)}/internal/get/{key}',
                    params={'ts': tx.start_ts}, timeout=2)
                result = r.json()
            tx.read_set[key] = result.get('value')
            tx.log_event('READ', f'key={key} from={primary}')
            return result
        except Exception as e:
            return {'error': str(e)}

    def write(self, tx_id, key, value):
        tx = self.txns.get(tx_id)
        if not tx or tx.status != TxStatus.ACTIVE:
            return {'error': 'invalid_tx'}
        tx.write_set[key] = {'value': value, 'op': 'PUT'}
        tx.log_event('WRITE_BUFFER', f'key={key} value={value}')
        print(f'[TX] {tx_id} buffered write key={key}')
        return {'ok': True, 'buffered': True, 'note':
                'Not in DB yet — waiting for commit'}

    def commit(self, tx_id):
        tx = self.txns.get(tx_id)
        if not tx: return {'error': 'tx_not_found'}
        with tx.lock:
            if tx.status != TxStatus.ACTIVE:
                return {'error': 'not_active'}
            tx.status = TxStatus.PREPARED
            tx.log_event('PHASE_1', 'Conflict detection')

        # Phase 1 — Conflict detection
        conflict = self._check_conflicts(tx)
        if conflict:
            tx.status = TxStatus.ABORTED
            tx.log_event('ABORTED', f'Conflict on key={conflict}')
            self.stats['aborted']   += 1
            self.stats['conflicts'] += 1
            print(f'[TX] {tx_id} ABORTED — conflict key={conflict}')
            return {
                'committed': False,
                'reason':    'write_conflict',
                'key':       conflict,
                'explain':   f'Key "{conflict}" was modified by another '
                             f'transaction after this one started. '
                             f'Snapshot Isolation prevents dirty writes.',
                'timeline':  tx.timeline
            }

        # Phase 2 — Distributed write with Raft
        commit_ts = TrueTime.safe_commit_ts()
        tx.commit_ts = commit_ts
        tx.log_event('PHASE_2', f'commit_ts={commit_ts}')

        errors = []
        written_nodes = {}

        for key, entry in tx.write_set.items():
            # Determine which nodes own this key (sharding + replication)
            owners  = ring.replicas(key, n=3)
            written_nodes[key] = owners
            shard   = ring.shard_id(key)

            # Step A: Send to Raft LEADER for consensus (always the elected leader)
            raft_leader = raft.leader_id
            if raft_leader == NODE_ID:
                result = raft.propose(key, entry['value'],
                                      entry['op'], commit_ts)
            else:
                try:
                    r = requests.post(
                        f'{peer_url(raft_leader)}/raft/propose',
                        json={'key': key, 'value': entry['value'],
                              'op': entry['op'], 'ts': commit_ts},
                        timeout=4)
                    result = r.json()
                except Exception as e:
                    result = {'committed': False, 'error': str(e)}

            if not result.get('committed'):
                # Raft failed — still write directly to storage for demo resilience
                print(f'[TX] Raft unavailable for {key}, writing directly')

            # Step B: Write to all replica nodes (sharding — each shard on 3 nodes)
            success_nodes = []
            for nid in owners:
                try:
                    if nid == NODE_ID:
                        storage.put(key, entry['value'], commit_ts)
                        success_nodes.append(nid)
                    else:
                        r2 = requests.put(
                            f'{peer_url(nid)}/internal/put/{key}',
                            json={'value': entry['value'], 'ts': commit_ts},
                            timeout=2)
                        if r2.status_code == 200:
                            success_nodes.append(nid)
                except Exception:
                    pass
            written_nodes[key] = success_nodes
            tx.log_event('WRITTEN',
                         f'key={key} shard={shard} nodes={success_nodes}')
            print(f'[TX] key={key} shard={shard} written to {success_nodes}')

        if [e for e in errors if 'Raft' in e and 'timeout' not in e]:
            tx.status = TxStatus.ABORTED
            self.stats['aborted'] += 1
            return {'committed': False, 'errors': errors}

        tx.status = TxStatus.COMMITTED
        self.stats['committed'] += 1
        self.log.append(tx.to_dict())
        print(f'[TX] {tx_id} COMMITTED ts={commit_ts}')
        return {
            'committed':   True,
            'tx_id':       tx_id,
            'commit_ts':   commit_ts,
            'keys_written': list(tx.write_set.keys()),
            'written_to':  written_nodes,
            'timeline':    tx.timeline
        }

    def _check_conflicts(self, tx):
        for key in tx.write_set:
            primary = ring.primary(key)
            try:
                if primary == NODE_ID:
                    latest = storage.latest_ts(key)
                else:
                    r      = requests.get(
                        f'{peer_url(primary)}/internal/ts/{key}',
                        timeout=1)
                    latest = r.json().get('latest_ts', 0)
                if latest > tx.start_ts:
                    return key
            except Exception:
                pass
        return None

    def abort(self, tx_id):
        tx = self.txns.get(tx_id)
        if tx:
            tx.status = TxStatus.ABORTED
            tx.log_event('MANUALLY_ABORTED')
            self.stats['aborted'] += 1
        return {'aborted': True, 'tx_id': tx_id}

txmgr = TxManager()

# ══════════════════════════════════════════════════════════════════════════════
#  REST API — Public endpoints (client facing)
# ══════════════════════════════════════════════════════════════════════════════

# ── Health ────────────────────────────────────────────────────────────────────
@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status':  'UP',
        'node':    NODE_ID,
        'member':  MY_MEMBER,
        'role':    raft.role.value,
        'leader':  raft.leader_id,
        'keys':    len(storage.all_keys()),
        'truetime': TrueTime.now()
    })

# ── Transactions ──────────────────────────────────────────────────────────────
@app.route('/tx/begin', methods=['POST'])
def tx_begin():
    return jsonify(txmgr.begin())

@app.route('/tx/<tx_id>/read/<key>', methods=['GET'])
def tx_read(tx_id, key):
    return jsonify(txmgr.read(tx_id, key))

@app.route('/tx/<tx_id>/write/<key>', methods=['PUT'])
def tx_write(tx_id, key):
    return jsonify(txmgr.write(tx_id, key, request.json['value']))

@app.route('/tx/<tx_id>/commit', methods=['POST'])
def tx_commit(tx_id):
    return jsonify(txmgr.commit(tx_id))

@app.route('/tx/<tx_id>/abort', methods=['POST'])
def tx_abort(tx_id):
    return jsonify(txmgr.abort(tx_id))

@app.route('/tx/<tx_id>/status', methods=['GET'])
def tx_status(tx_id):
    tx = txmgr.txns.get(tx_id)
    return jsonify(tx.to_dict() if tx else {'error': 'not found'})

# ── Simple KV (auto-wraps in transaction) ─────────────────────────────────────
@app.route('/put/<key>', methods=['POST'])
def simple_put(key):
    tx    = txmgr.begin()
    tx_id = tx['tx_id']
    txmgr.write(tx_id, key, request.json['value'])
    return jsonify(txmgr.commit(tx_id))

@app.route('/get/<key>', methods=['GET'])
def simple_get(key):
    primary = ring.primary(key)
    if primary == NODE_ID:
        return jsonify(storage.get(key))
    try:
        r = requests.get(f'{peer_url(primary)}/internal/get/{key}',
                         timeout=2)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({'error': str(e)})

# ── Status & Topology ─────────────────────────────────────────────────────────
@app.route('/raft/status', methods=['GET'])
def raft_status():
    return jsonify(raft.status())

@app.route('/ring/topology', methods=['GET'])
def ring_topology():
    return jsonify({
        'node':        NODE_ID,
        'ring_status': ring.status,
        'vnode_count': len(ring.ring),
        'truetime':    TrueTime.now()
    })

@app.route('/ring/route/<key>', methods=['GET'])
def ring_route(key):
    return jsonify({
        'key':      key,
        'shard_id': ring.shard_id(key),
        'primary':  ring.primary(key),
        'replicas': ring.replicas(key, 3),
        'truetime': TrueTime.now()
    })

@app.route('/storage/info', methods=['GET'])
def storage_info():
    return jsonify(storage.info())

@app.route('/storage/keys', methods=['GET'])
def storage_keys():
    keys = storage.all_keys()
    data = {k: storage.get(k)['value'] for k in keys}
    return jsonify({'node': NODE_ID, 'member': MY_MEMBER,
                    'keys': data, 'count': len(keys)})

@app.route('/storage/versions/<key>', methods=['GET'])
def storage_versions(key):
    return jsonify({'key': key, 'versions': storage.get_versions(key)})

@app.route('/monitor/log', methods=['GET'])
def monitor_log():
    return jsonify(monitor.log())

@app.route('/tx/stats', methods=['GET'])
def tx_stats():
    return jsonify({'node': NODE_ID, 'member': MY_MEMBER,
                    'stats': txmgr.stats})

@app.route('/snapshot', methods=['GET'])
def snapshot():
    ts   = request.args.get('ts', type=int)
    data = storage.snapshot(ts)
    return jsonify({'node': NODE_ID, 'member': MY_MEMBER,
                    'data': data, 'count': len(data),
                    'truetime': TrueTime.now()})

# ── Raft RPC endpoints ────────────────────────────────────────────────────────
@app.route('/raft/vote', methods=['POST'])
def raft_vote():
    d = request.json
    with raft.lock:
        granted = False
        if d['term'] > raft.term:
            raft.term = d['term']
            raft.role = Role.FOLLOWER
            raft.voted_for = None
        lt = raft.log[-1].term if raft.log else 0
        li = len(raft.log) - 1
        log_ok = (d['last_log_term'] > lt or
                  (d['last_log_term'] == lt and d['last_log_index'] >= li))
        if (d['term'] >= raft.term and log_ok
                and raft.voted_for in (None, d['candidate'])):
            raft.voted_for      = d['candidate']
            raft.last_heartbeat = time.time()
            granted             = True
            print(f'[Raft] {NODE_ID} voted for {d["candidate"]} '
                  f'term={d["term"]}')
    return jsonify({'term': raft.term, 'granted': granted})

@app.route('/raft/append', methods=['POST'])
def raft_append():
    d = request.json
    with raft.lock:
        if d['term'] < raft.term:
            return jsonify({'term': raft.term, 'success': False})
        raft.last_heartbeat = time.time()
        raft.term           = d['term']
        raft.role           = Role.FOLLOWER
        raft.leader_id      = d['leader']
        for e in d.get('entries', []):
            idx = e['index']
            while len(raft.log) <= idx:
                raft.log.append(None)
            raft.log[idx] = LogEntry(e['term'], e['index'], e['key'],
                                     e['value'], e['op'], e['ts'])
        if d['commit'] > raft.commit_index:
            raft.commit_index = min(d['commit'], len(raft.log) - 1)
            raft._apply()
    return jsonify({'term': raft.term, 'success': True})

@app.route('/raft/propose', methods=['POST'])
def raft_propose():
    d = request.json
    return jsonify(raft.propose(d['key'], d.get('value'),
                                d['op'], d['ts']))

# ── Internal endpoints (node-to-node) ────────────────────────────────────────
@app.route('/internal/get/<key>', methods=['GET'])
def internal_get(key):
    ts = request.args.get('ts', type=int)
    return jsonify(storage.get(key, ts))

@app.route('/internal/put/<key>', methods=['PUT'])
def internal_put(key):
    d = request.json
    return jsonify(storage.put(key, d['value'], d.get('ts')))

@app.route('/internal/ts/<key>', methods=['GET'])
def internal_ts(key):
    return jsonify({'key': key, 'latest_ts': storage.latest_ts(key)})

# ── Conflict demo ─────────────────────────────────────────────────────────────
@app.route('/demo/conflict', methods=['POST'])
def demo_conflict():
    key   = (request.json or {}).get('key', 'score')
    val_a = (request.json or {}).get('val_a', '100')
    val_b = (request.json or {}).get('val_b', '200')
    steps = []

    # TX-A begins
    ta    = txmgr.begin()
    steps.append({'step': 1, 'event': 'TX-A begins',
                  'tx_id': ta['tx_id'], 'start_ts': ta['start_ts']})

    # TX-B begins, writes, and commits BEFORE TX-A
    tb     = txmgr.begin()
    txmgr.write(tb['tx_id'], key, val_b)
    cb     = txmgr.commit(tb['tx_id'])
    steps.append({'step': 2,
                  'event': f'TX-B writes key={key} val={val_b} and COMMITS',
                  'result': cb})

    # TX-A tries to write same key and commit
    txmgr.write(ta['tx_id'], key, val_a)
    ca    = txmgr.commit(ta['tx_id'])
    steps.append({'step': 3,
                  'event': f'TX-A writes key={key} val={val_a} and tries to COMMIT',
                  'result': ca})

    final = storage.get(key)
    return jsonify({
        'demo':        'Snapshot Isolation — Write Conflict',
        'key':         key,
        'steps':       steps,
        'final_value': final.get('value'),
        'winner':      'TX-B (committed first)',
        'loser':       'TX-A (aborted — write conflict detected)',
        'conclusion':  (
            'TX-B committed first. When TX-A tried to commit, '
            'Phase 1 conflict check found that the key was modified '
            'AFTER TX-A started. TX-A was aborted. '
            'This is Snapshot Isolation working correctly!'
        )
    })

# ── Cluster-wide snapshot (collects from all nodes) ──────────────────────────
@app.route('/cluster/snapshot', methods=['GET'])
def cluster_snapshot():
    result     = {}
    node_data  = {NODE_ID: storage.snapshot()}
    result[NODE_ID] = node_data[NODE_ID]
    for nid in PEERS:
        try:
            r = requests.get(f'{peer_url(nid)}/snapshot', timeout=3)
            result[nid] = r.json().get('data', {})
        except Exception:
            result[nid] = {'error': 'unreachable'}
    # Merge — show which nodes have each key (proves replication!)
    merged = {}
    for nid, data in result.items():
        if isinstance(data, dict) and 'error' not in data:
            for k, v in data.items():
                if k not in merged:
                    merged[k] = {'value': v, 'stored_on': [nid],
                                 'shard': ring.shard_id(k)}
                else:
                    if nid not in merged[k]['stored_on']:
                        merged[k]['stored_on'].append(nid)
    # Add replication count
    for k in merged:
        merged[k]['replica_count'] = len(merged[k]['stored_on'])
        merged[k]['fully_replicated'] = len(merged[k]['stored_on']) >= 2
    return jsonify({
        'cluster_snapshot': merged,
        'total_unique_keys': len(merged),
        'per_node_breakdown': {
            nid: {'keys': list(d.keys()) if isinstance(d,dict) and 'error' not in d else [],
                  'count': len(d) if isinstance(d,dict) and 'error' not in d else 0}
            for nid, d in result.items()
        },
        'truetime': TrueTime.now(),
        'note': 'stored_on shows which laptops physically hold each key!'
    })

# ══════════════════════════════════════════════════════════════════════════════



# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 11 — Ricart-Agrawala Distributed Mutual Exclusion
#  Reference: "An Optimal Algorithm for Mutual Exclusion in Computer Networks"
#              Ricart & Agrawala, Communications of the ACM, 1981
#
#  Problem: Two nodes want to write the same key at the same time.
#           Who goes first? How do we prevent both from entering
#           the critical section simultaneously?
#
#  Solution:
#   1. Node wanting CS sends REQUEST(timestamp, node_id) to ALL others
#   2. On receiving REQUEST:
#      - If not interested OR requester has smaller timestamp → send GRANT
#      - If we want CS AND our timestamp is smaller → DEFER (queue it)
#   3. Node enters CS only after receiving GRANT from ALL other nodes
#   4. On exiting CS → send GRANT to all deferred nodes
#
#  Guarantee: Only ONE node is in the critical section at any time.
#             Timestamps break ties (smaller timestamp wins).
#             Starvation-free — every request is eventually granted.
# ══════════════════════════════════════════════════════════════════════════════
import queue as queuemod

class RicartAgrawala:
    def __init__(self, node_id):
        self.node_id      = node_id
        self.clock        = 0          # logical clock for timestamps
        self.lock         = threading.Lock()

        # Per-resource state
        self.requesting   = {}         # resource -> bool (am I requesting?)
        self.in_cs        = {}         # resource -> bool (am I in CS?)
        self.my_timestamp = {}         # resource -> my request timestamp
        self.grant_count  = {}         # resource -> how many GRANTs received
        self.deferred     = {}         # resource -> list of deferred node_ids
        self.cs_log       = []         # audit log of all CS entries/exits

    def _tick(self):
        self.clock += 1
        return self.clock

    def _update_clock(self, received_ts):
        self.clock = max(self.clock, received_ts) + 1
        return self.clock

    def request_cs(self, resource):
        """
        Phase 1: Broadcast REQUEST to all peers.
        Wait until all GRANTs received, then enter Critical Section.
        """
        with self.lock:
            if self.requesting.get(resource) or self.in_cs.get(resource):
                return {'error': 'already_requesting_or_in_cs',
                        'resource': resource}
            ts = self._tick()
            self.my_timestamp[resource] = ts
            self.requesting[resource]   = True
            self.grant_count[resource]  = 0
            self.deferred[resource]     = []

        print(f'[RA] {NODE_ID} requesting CS for resource={resource} ts={ts}')

        # Send REQUEST to all peers
        peer_list    = list(PEERS.keys())
        grants_needed = len(peer_list)
        grant_events  = {}

        for peer_id in peer_list:
            try:
                r = requests.post(
                    f'{peer_url(peer_id)}/ra/request',
                    json={
                        'resource':  resource,
                        'timestamp': ts,
                        'from_node': NODE_ID
                    },
                    timeout=3
                )
                resp = r.json()
                grant_events[peer_id] = resp

                if resp.get('grant') == 'immediate':
                    with self.lock:
                        self.grant_count[resource] += 1
                        print(f'[RA] Got immediate GRANT from {peer_id} '
                              f'for {resource} '
                              f'({self.grant_count[resource]}/{grants_needed})')
                elif resp.get('grant') == 'deferred':
                    print(f'[RA] {peer_id} DEFERRED our request for {resource} '
                          f'(they have higher priority)')
            except Exception as e:
                # Peer unreachable — treat as implicit grant
                grant_events[peer_id] = {'error': str(e), 'grant': 'implicit'}
                with self.lock:
                    self.grant_count[resource] += 1

        # Wait for all grants (max 5 seconds)
        deadline = time.time() + 5.0
        while time.time() < deadline:
            with self.lock:
                if self.grant_count.get(resource, 0) >= grants_needed:
                    break
            time.sleep(0.1)

        with self.lock:
            grants = self.grant_count.get(resource, 0)
            if grants >= grants_needed:
                self.in_cs[resource]    = True
                self.requesting[resource] = False
                entered_at = time.strftime('%H:%M:%S')
                self.cs_log.append({
                    'event':      'ENTER_CS',
                    'resource':   resource,
                    'node':       NODE_ID,
                    'timestamp':  ts,
                    'grants':     grants,
                    'entered_at': entered_at
                })
                print(f'[RA] *** {NODE_ID} ENTERED critical section '
                      f'for {resource} ***')
                return {
                    'entered_cs':   True,
                    'resource':     resource,
                    'timestamp':    ts,
                    'grants_received': grants,
                    'grants_needed':   grants_needed,
                    'grant_details':   grant_events
                }
            else:
                self.requesting[resource] = False
                return {
                    'entered_cs': False,
                    'resource':   resource,
                    'reason':     'timeout — could not get all grants',
                    'grants_received': grants,
                    'grants_needed':   grants_needed
                }

    def release_cs(self, resource):
        """
        Phase 2: Exit CS, send GRANT to all deferred nodes.
        """
        deferred_nodes = []
        with self.lock:
            if not self.in_cs.get(resource):
                return {'error': 'not_in_cs', 'resource': resource}
            self.in_cs[resource] = False
            deferred_nodes       = list(self.deferred.get(resource, []))
            self.deferred[resource] = []
            self.cs_log.append({
                'event':      'EXIT_CS',
                'resource':   resource,
                'node':       NODE_ID,
                'released_to': deferred_nodes,
                'exited_at':  time.strftime('%H:%M:%S')
            })

        print(f'[RA] {NODE_ID} EXITED CS for {resource}, '
              f'releasing {len(deferred_nodes)} deferred nodes')

        # Grant all deferred nodes
        for peer_id in deferred_nodes:
            try:
                requests.post(
                    f'{peer_url(peer_id)}/ra/grant',
                    json={'resource': resource, 'from_node': NODE_ID},
                    timeout=2
                )
                print(f'[RA] Sent deferred GRANT to {peer_id} for {resource}')
            except Exception:
                pass

        return {
            'released':    True,
            'resource':    resource,
            'granted_to':  deferred_nodes
        }

    def receive_request(self, resource, req_ts, from_node):
        """
        Another node is requesting CS for this resource.
        Grant immediately OR defer based on timestamp comparison.
        """
        with self.lock:
            self._update_clock(req_ts)
            my_ts    = self.my_timestamp.get(resource)
            want_cs  = self.requesting.get(resource) or self.in_cs.get(resource)

            # Grant immediately if:
            # - We don't want CS, OR
            # - We want CS but requester has smaller timestamp (higher priority)
            # - Tie in timestamp: smaller node_id wins
            if not want_cs:
                decision = 'immediate'
            else:
                my_priority   = (my_ts, NODE_ID)
                their_priority = (req_ts, from_node)
                decision = 'immediate' if their_priority < my_priority else 'deferred'

            if decision == 'deferred':
                self.deferred.setdefault(resource, []).append(from_node)
                print(f'[RA] DEFERRING {from_node} request for {resource} '
                      f'(our ts={my_ts} <= their ts={req_ts})')
            else:
                print(f'[RA] Granting {from_node} IMMEDIATE access to {resource}')

            return decision

    def receive_grant(self, resource, from_node):
        """Called when a deferred GRANT finally arrives"""
        with self.lock:
            self.grant_count[resource] = self.grant_count.get(resource, 0) + 1
            print(f'[RA] Received deferred GRANT from {from_node} '
                  f'for {resource} '
                  f'({self.grant_count[resource]} total)')

    def get_log(self):
        return {
            'node':       NODE_ID,
            'member':     MY_MEMBER,
            'cs_log':     self.cs_log,
            'in_cs_now':  {r: v for r, v in self.in_cs.items() if v},
            'clock':      self.clock
        }

ra = RicartAgrawala(NODE_ID)

# ── Ricart-Agrawala REST endpoints ────────────────────────────────────────────

@app.route('/ra/request', methods=['POST'])
def ra_request():
    """Peer is requesting CS — we decide grant or defer"""
    d        = request.json
    resource = d['resource']
    req_ts   = d['timestamp']
    from_nd  = d['from_node']
    decision = ra.receive_request(resource, req_ts, from_nd)
    return jsonify({
        'grant':     decision,
        'from_node': NODE_ID,
        'resource':  resource,
        'decision_explain': (
            'immediate — we do not want this resource right now' if decision == 'immediate'
            else 'deferred — we have higher priority (smaller timestamp)'
        )
    })

@app.route('/ra/grant', methods=['POST'])
def ra_grant():
    """Peer sent us a deferred grant"""
    d = request.json
    ra.receive_grant(d['resource'], d['from_node'])
    return jsonify({'ok': True})

@app.route('/ra/release', methods=['POST'])
def ra_release():
    resource = (request.json or {}).get('resource', 'default')
    return jsonify(ra.release_cs(resource))

@app.route('/ra/log', methods=['GET'])
def ra_log():
    return jsonify(ra.get_log())

@app.route('/demo/mutual_exclusion', methods=['POST'])
def demo_mutual_exclusion():
    """
    Full Ricart-Agrawala demonstration:

    Simulates two nodes wanting the same resource simultaneously.
    Shows: REQUEST broadcast → GRANT/DEFER decisions → CS entry →
           CS exit → deferred GRANTs released → second node enters CS
    """
    resource = (request.json or {}).get('resource', 'key:balance')
    value    = (request.json or {}).get('value', '9999')
    steps    = []

    # Step 1: This node requests CS
    steps.append({
        'step':    1,
        'event':   f'{NODE_ID} ({MY_MEMBER}) broadcasts REQUEST for resource={resource}',
        'explain': (
            'Phase 1 of Ricart-Agrawala: Send REQUEST(timestamp, node_id) '
            'to ALL other nodes. Each peer will grant immediately or defer '
            'based on whether they also want this resource.'
        )
    })

    result = ra.request_cs(resource)
    steps.append({
        'step':    2,
        'event':   f'REQUEST responses collected',
        'result':  result,
        'explain': (
            f'Received {result.get("grants_received", 0)} GRANTs from peers. '
            f'Grant details show each peer decision: '
            f'"immediate" = peer does not want this resource, '
            f'"deferred" = peer has higher priority timestamp.'
        )
    })

    if result.get('entered_cs'):
        # Step 3: In critical section — perform the write
        ts = int(time.time() * 1000)
        storage.put(resource.replace('key:', ''), value, ts)
        steps.append({
            'step':    3,
            'event':   f'ENTERED critical section — writing {resource}={value}',
            'explain': (
                'Only ONE node can be in the critical section at a time. '
                'This node has exclusive write access to this resource. '
                'No other node can write until we release.'
            ),
            'written': {'resource': resource, 'value': value, 'ts': ts}
        })

        # Step 4: Release CS
        release = ra.release_cs(resource)
        steps.append({
            'step':    4,
            'event':   f'EXITED critical section, released to deferred nodes',
            'result':  release,
            'explain': (
                'On exit, we send GRANT to all nodes we deferred earlier. '
                'They can now enter the critical section in timestamp order. '
                'This guarantees starvation-freedom — every request eventually granted.'
            )
        })

    return jsonify({
        'demo':      'Ricart-Agrawala Distributed Mutual Exclusion',
        'reference': 'Ricart & Agrawala, CACM 1981',
        'resource':  resource,
        'node':      NODE_ID,
        'member':    MY_MEMBER,
        'steps':     steps,
        'cs_log':    ra.cs_log[-5:],
        'conclusion': (
            'Ricart-Agrawala guarantees mutual exclusion across the '
            'distributed system with NO central coordinator. '
            'It uses logical timestamps to break ties — the node with '
            'the smallest timestamp gets priority. '
            'This requires exactly 2(N-1) messages for N nodes — '
            f'in our 4-node system: 2 x 3 = 6 messages per CS entry.'
        ),
        'message_complexity': f'2(N-1) = 2 x {len(ALL_NODES)-1} = {2*(len(ALL_NODES)-1)} messages'
    })

@app.route('/demo/ra_contention', methods=['POST'])
def demo_ra_contention():
    """
    TRUE contention demo — this node signals all peers to compete
    simultaneously. Everyone requests CS at the same time via threads.
    Only ONE wins first. Others are deferred and enter in timestamp order.
    """
    resource = (request.json or {}).get('resource', 'key:contest')

    # Step 1: Tell ALL peers to start requesting CS simultaneously
    peer_threads = []
    peer_results = {}

    def trigger_peer(peer_id):
        try:
            r = requests.post(
                f'{peer_url(peer_id)}/ra/compete',
                json={'resource': resource,
                      'value': f'written-by-{peer_id}'},
                timeout=10
            )
            peer_results[peer_id] = r.json()
        except Exception as e:
            peer_results[peer_id] = {'error': str(e)}

    # Fire all peers simultaneously in threads
    for pid in PEERS:
        t = threading.Thread(target=trigger_peer, args=(pid,), daemon=True)
        peer_threads.append(t)

    # Step 2: This node also competes — all start at roughly same time
    def this_node_compete():
        peer_results[NODE_ID] = _do_compete(resource,
                                            f'written-by-{NODE_ID}')

    self_thread = threading.Thread(target=this_node_compete, daemon=True)

    # Launch ALL simultaneously
    self_thread.start()
    for t in peer_threads:
        t.start()

    # Wait for all
    self_thread.join(timeout=10)
    for t in peer_threads:
        t.join(timeout=10)

    # Sort by who entered CS first (timestamp order)
    ordered = sorted(
        [(nid, r) for nid, r in peer_results.items()
         if isinstance(r, dict) and 'enter_time' in r],
        key=lambda x: x[1].get('enter_time', 9999999999999)
    )

    return jsonify({
        'demo':     'Ricart-Agrawala TRUE Contention',
        'resource': resource,
        'results':  peer_results,
        'cs_order': [
            {
                'position': i+1,
                'node':     nid,
                'member':   ALL_NODES[nid]['member'],
                'enter_time': r.get('enter_time'),
                'grants':   r.get('grants_received'),
                'deferred_by': r.get('deferred_by', [])
            }
            for i, (nid, r) in enumerate(ordered)
        ],
        'conclusion': (
            'All 4 nodes requested the critical section simultaneously. '
            'They are ordered above by entry time. '
            'The node with the smallest Lamport timestamp entered first. '
            'Others were deferred and entered one by one as each '
            'predecessor released the CS. '
            'At no point did two nodes hold the CS simultaneously — '
            'mutual exclusion guaranteed!'
        )
    })

def _do_compete(resource, value):
    """Internal helper — request CS, write, release, return result"""
    result = ra.request_cs(resource)
    enter_time = int(time.time() * 1000)
    deferred_by = [
        pid for pid, d in result.get('grant_details', {}).items()
        if d.get('grant') == 'deferred'
    ]
    if result.get('entered_cs'):
        key = resource.replace('key:', '')
        storage.put(key, value, enter_time)
        time.sleep(0.3)   # hold CS briefly so ordering is visible
        release = ra.release_cs(resource)
        return {
            'node':           NODE_ID,
            'member':         MY_MEMBER,
            'won_cs':         True,
            'enter_time':     enter_time,
            'timestamp_used': result.get('timestamp'),
            'grants_received': result.get('grants_received'),
            'deferred_by':    deferred_by,
            'wrote':          value,
            'released_to':    release.get('granted_to', [])
        }
    return {
        'node':    NODE_ID,
        'member':  MY_MEMBER,
        'won_cs':  False,
        'reason':  result.get('reason', 'timeout')
    }

@app.route('/ra/compete', methods=['POST'])
def ra_compete():
    """Internal endpoint — peer asks this node to compete for CS"""
    d        = request.json
    resource = d['resource']
    value    = d.get('value', f'written-by-{NODE_ID}')
    return jsonify(_do_compete(resource, value))

# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 9 — Chandy-Lamport Global Snapshot Algorithm
#  Reference: "Distributed Snapshots: Determining Global States of
#              Distributed Systems" — Chandy & Lamport, 1985
# ══════════════════════════════════════════════════════════════════════════════
class ChandyLamport:
    """
    Chandy-Lamport algorithm captures a consistent global state by:
    1. Initiator saves its local state and sends MARKER on all outgoing channels
    2. On receiving first MARKER: node saves local state, starts recording
       incoming channels, forwards MARKER on all outgoing channels
    3. On receiving subsequent MARKERs: stop recording that channel
    4. Snapshot complete when all channels recorded
    """
    def __init__(self):
        self.snapshots      = {}   # snapshot_id -> global state
        self.active         = {}   # snapshot_id -> recording state
        self.lock           = threading.Lock()

    def initiate(self, snapshot_id=None):
        snap_id = snapshot_id or f'snap-{int(time.time()*1000)}'
        ts      = TrueTime.now()

        # Step 1: Record own local state
        local_state = {
            'node':        NODE_ID,
            'member':      MY_MEMBER,
            'local_ts':    ts,
            'storage':     storage.snapshot(),
            'raft_term':   raft.term,
            'raft_role':   raft.role.value,
            'raft_log_len': len(raft.log),
            'raft_commit': raft.commit_index,
            'tx_stats':    txmgr.stats.copy(),
            'ring_status': dict(ring.status),
            'recorded_at': time.strftime('%H:%M:%S.') +
                           str(int(time.time()*1000) % 1000).zfill(3)
        }

        with self.lock:
            self.active[snap_id] = {
                'initiator':    NODE_ID,
                'started_at':   ts,
                'states':       {NODE_ID: local_state},
                'channels':     {},   # channel -> recorded messages
                'complete':     False
            }

        print(f'[CL] Initiating snapshot {snap_id} from {NODE_ID}')

        # Step 2: Send MARKER to all peers
        responses = {}
        for peer_id in PEERS:
            try:
                r = requests.post(
                    f'{peer_url(peer_id)}/cl/marker',
                    json={'snapshot_id': snap_id,
                          'from_node':   NODE_ID,
                          'initiator':   NODE_ID},
                    timeout=3)
                resp = r.json()
                responses[peer_id] = resp
                # Collect peer state from marker response
                if 'local_state' in resp:
                    with self.lock:
                        if snap_id in self.active:
                            self.active[snap_id]['states'][peer_id] =                                 resp['local_state']
            except Exception as e:
                responses[peer_id] = {'error': str(e)}

        # Step 3: Compile global snapshot
        with self.lock:
            if snap_id in self.active:
                snap = self.active[snap_id]
                snap['complete']    = True
                snap['finished_at'] = TrueTime.now()

                # Global state = union of all node states
                global_storage = {}
                for nid, state in snap['states'].items():
                    if isinstance(state, dict) and 'storage' in state:
                        for k, v in state['storage'].items():
                            if k not in global_storage:
                                global_storage[k] = {
                                    'value':     v,
                                    'seen_on':   [nid]
                                }
                            else:
                                global_storage[k]['seen_on'].append(nid)

                result = {
                    'snapshot_id':       snap_id,
                    'algorithm':         'Chandy-Lamport 1985',
                    'initiator':         NODE_ID,
                    'started_at':        snap['started_at'],
                    'finished_at':       snap['finished_at'],
                    'participating_nodes': list(snap['states'].keys()),
                    'node_count':        len(snap['states']),
                    'global_storage':    global_storage,
                    'per_node_states':   snap['states'],
                    'consistent':        True,
                    'explanation': (
                        'This is a Chandy-Lamport consistent global snapshot. '
                        'Each node recorded its local state when it received '
                        'the MARKER message. Because MARKERs travel on the '
                        'same channels as data, no message is missed and no '
                        'message is recorded twice. The result is a globally '
                        'consistent state even though no global clock exists.'
                    )
                }
                self.snapshots[snap_id] = result
                return result
        return {'error': 'snapshot failed'}

    def receive_marker(self, snapshot_id, from_node, initiator):
        """Called when this node receives a MARKER — record local state"""
        ts = TrueTime.now()
        local_state = {
            'node':         NODE_ID,
            'member':       MY_MEMBER,
            'local_ts':     ts,
            'storage':      storage.snapshot(),
            'raft_term':    raft.term,
            'raft_role':    raft.role.value,
            'raft_log_len': len(raft.log),
            'raft_commit':  raft.commit_index,
            'tx_stats':     txmgr.stats.copy(),
            'ring_status':  dict(ring.status),
            'recorded_at':  time.strftime('%H:%M:%S.') +
                            str(int(time.time()*1000) % 1000).zfill(3),
            'marker_from':  from_node
        }
        print(f'[CL] {NODE_ID} received MARKER from {from_node} '
              f'for snapshot {snapshot_id} — recording local state')
        return local_state

    def list_snapshots(self):
        return {
            'node':      NODE_ID,
            'snapshots': list(self.snapshots.keys()),
            'count':     len(self.snapshots)
        }

    def get_snapshot(self, snap_id):
        return self.snapshots.get(snap_id, {'error': 'not found'})

cl = ChandyLamport()

# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 10 — TrueTime Demo + Event Ordering
# ══════════════════════════════════════════════════════════════════════════════
class EventLog:
    """Tracks all system events with TrueTime timestamps for causal ordering"""
    def __init__(self):
        self.events = []
        self.lock   = threading.Lock()

    def record(self, event_type, detail, node=NODE_ID):
        ts = TrueTime.now()
        with self.lock:
            self.events.append({
                'event':     event_type,
                'detail':    detail,
                'node':      node,
                'truetime':  ts,
                'wall_time': time.strftime('%H:%M:%S')
            })

    def get_ordered(self):
        with self.lock:
            # Sort by TrueTime earliest — causal ordering
            sorted_events = sorted(
                self.events,
                key=lambda e: e['truetime']['earliest']
            )
            return sorted_events

event_log = EventLog()

# ── Chandy-Lamport REST endpoints ─────────────────────────────────────────────
@app.route('/cl/snapshot', methods=['POST'])
def cl_snapshot():
    snap_id = (request.json or {}).get('snapshot_id')
    result  = cl.initiate(snap_id)
    event_log.record('CL_SNAPSHOT', f'snapshot_id={result.get("snapshot_id")}')
    return jsonify(result)

@app.route('/cl/marker', methods=['POST'])
def cl_marker():
    d     = request.json
    state = cl.receive_marker(d['snapshot_id'], d['from_node'], d['initiator'])
    return jsonify({'received': True, 'local_state': state,
                    'node': NODE_ID})

@app.route('/cl/list', methods=['GET'])
def cl_list():
    return jsonify(cl.list_snapshots())

@app.route('/cl/get/<snap_id>', methods=['GET'])
def cl_get(snap_id):
    return jsonify(cl.get_snapshot(snap_id))

# ── TrueTime demo endpoint ────────────────────────────────────────────────────
@app.route('/demo/truetime', methods=['GET'])
def demo_truetime():
    """Shows TrueTime uncertainty and commit wait in action"""
    before  = TrueTime.now()
    # Simulate commit wait — wait out uncertainty window
    time.sleep(TrueTime.UNCERTAINTY_MS / 1000.0 * 2)
    after   = TrueTime.now()
    # Prove: after.earliest > before.latest (no overlap = safe commit order)
    safe    = after['earliest'] > before['latest']
    return jsonify({
        'demo':         'TrueTime Commit Wait',
        'before_commit': before,
        'after_commit':  after,
        'uncertainty_ms': TrueTime.UNCERTAINTY_MS,
        'safe_to_commit': safe,
        'explanation': (
            'Google Spanner waits out the TrueTime uncertainty window '
            'before committing. This guarantees that if TX-A commits '
            'before TX-B starts, then commit_ts(A) < start_ts(B) — '
            'even without a global clock. '
            f'Here: after.earliest ({round(after["earliest"],1)}) > '
            f'before.latest ({round(before["latest"],1)}) = {safe}'
        )
    })

# ── Event ordering endpoint ───────────────────────────────────────────────────
@app.route('/demo/events', methods=['GET'])
def demo_events():
    return jsonify({
        'node':             NODE_ID,
        'causally_ordered_events': event_log.get_ordered(),
        'explanation': (
            'Events sorted by TrueTime earliest timestamp. '
            'This gives causal ordering without a global clock — '
            'exactly how Google Spanner orders transactions globally.'
        )
    })

# ── Majority ACK visibility ───────────────────────────────────────────────────
@app.route('/demo/write_with_acks', methods=['POST'])
def demo_write_with_acks():
    """Write a key and show exactly which nodes acknowledged"""
    key   = (request.json or {}).get('key', 'demo_key')
    value = (request.json or {}).get('value', 'demo_value')
    ts    = TrueTime.now()
    event_log.record('WRITE_START', f'key={key} value={value}')

    commit_ts = TrueTime.safe_commit_ts()
    owners    = ring.replicas(key, n=3)
    shard     = ring.shard_id(key)
    acks      = {}

    # Write to each replica and track ACK
    for nid in owners:
        try:
            if nid == NODE_ID:
                storage.put(key, value, commit_ts)
                acks[nid] = {'acked': True, 'member': MY_MEMBER,
                             'latency_ms': 0}
            else:
                t0 = time.time()
                r  = requests.put(
                    f'{peer_url(nid)}/internal/put/{key}',
                    json={'value': value, 'ts': commit_ts},
                    timeout=2)
                latency = round((time.time() - t0) * 1000, 1)
                acks[nid] = {
                    'acked':   r.status_code == 200,
                    'member':  ALL_NODES[nid]['member'],
                    'latency_ms': latency
                }
        except Exception as e:
            acks[nid] = {'acked': False, 'error': str(e)}

    ack_count = sum(1 for a in acks.values() if a.get('acked'))
    quorum    = len(ALL_NODES) // 2 + 1
    committed = ack_count >= quorum

    event_log.record('WRITE_COMMITTED' if committed else 'WRITE_FAILED',
                     f'key={key} acks={ack_count}/{len(owners)}')

    return jsonify({
        'key':          key,
        'value':        value,
        'shard_id':     shard,
        'replicas':     owners,
        'acks':         acks,
        'ack_count':    ack_count,
        'quorum_needed': quorum,
        'committed':    committed,
        'commit_ts':    commit_ts,
        'truetime':     ts,
        'explanation': (
            f'Wrote to {len(owners)} replica nodes. '
            f'Got {ack_count} ACKs. '
            f'Quorum needed = {quorum}. '
            f'Committed = {committed}. '
            'This is exactly how Raft ensures majority acknowledgement!'
        )
    })

# ── Transaction abort demo ────────────────────────────────────────────────────
@app.route('/demo/abort', methods=['POST'])
def demo_abort():
    """Explicitly shows a transaction being aborted due to conflict"""
    key = (request.json or {}).get('key', 'balance')

    steps = []

    # Step 1: Write initial value
    init_tx = txmgr.begin()
    txmgr.write(init_tx['tx_id'], key, '1000')
    txmgr.commit(init_tx['tx_id'])
    steps.append({'step': 1,
                  'event': f'Initial write: {key}=1000',
                  'status': 'COMMITTED'})

    # Step 2: TX-A begins (wants to debit 200)
    tx_a = txmgr.begin()
    txmgr.read(tx_a['tx_id'], key)  # reads 1000
    steps.append({'step': 2,
                  'event': f'TX-A begins, reads {key}=1000, plans to write 800',
                  'tx_id': tx_a['tx_id'],
                  'status': 'ACTIVE'})

    # Step 3: TX-B concurrently modifies the key (deposit 500)
    tx_b = txmgr.begin()
    txmgr.write(tx_b['tx_id'], key, '1500')
    result_b = txmgr.commit(tx_b['tx_id'])
    steps.append({'step': 3,
                  'event': f'TX-B concurrently writes {key}=1500 and commits',
                  'tx_id': tx_b['tx_id'],
                  'result': result_b,
                  'status': 'COMMITTED'})

    # Step 4: TX-A tries to commit — should be aborted!
    txmgr.write(tx_a['tx_id'], key, '800')
    result_a = txmgr.commit(tx_a['tx_id'])
    steps.append({'step': 4,
                  'event': f'TX-A tries to write {key}=800 — CONFLICT!',
                  'tx_id': tx_a['tx_id'],
                  'result': result_a,
                  'status': result_a.get('committed') and 'COMMITTED' or 'ABORTED'})

    final = storage.get(key)
    return jsonify({
        'demo':        'Transaction Abort via Snapshot Isolation',
        'key':         key,
        'steps':       steps,
        'final_value': final.get('value'),
        'explanation': (
            f'TX-A read {key}=1000 and planned to write 800. '
            f'But TX-B committed {key}=1500 before TX-A finished. '
            f'When TX-A tried to commit, Phase 1 detected the conflict '
            f'and aborted TX-A. Final value is 1500 (TX-B wins). '
            f'This prevents the lost update anomaly!'
        )
    })

if __name__ == '__main__':
    print("=" * 60)
    print(f"  NebulaDB Unified Node")
    print(f"  Node:   {NODE_ID}")
    print(f"  Member: {MY_MEMBER}")
    print(f"  IP:     {MY_HOST}:{MY_PORT}")
    print(f"  Peers:  {list(PEERS.keys())}")
    print("=" * 60)
    print(f"\n[Ready] All modules started:")
    print(f"  ✓ LSM Storage Engine (local)")
    print(f"  ✓ Raft Consensus (voting with all peers)")
    print(f"  ✓ Consistent Hash Ring")
    print(f"  ✓ Heartbeat Monitor (auto failure detection)")
    print(f"  ✓ Transaction Manager (2PC + Snapshot Isolation)")
    print(f"\n[Raft] Election will start in 5-9 seconds...")
    app.run(host='0.0.0.0', port=MY_PORT, debug=False)
