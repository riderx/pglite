import PostgresModFactory from '../release/pglite'

type IDBFS = Emscripten.FileSystemType & {
  quit: () => void
  dbs: Record<string, IDBDatabase>
}

export type FS = typeof FS & {
  filesystems: {
    MEMFS: Emscripten.FileSystemType
    NODEFS: Emscripten.FileSystemType
    IDBFS: IDBFS
  }
  quit: () => void
}

export interface PostgresMod
  extends Omit<EmscriptenModule, 'preInit' | 'preRun' | 'postRun'> {
  preInit: Array<{ (mod: PostgresMod): void }>
  preRun: Array<{ (mod: PostgresMod): void }>
  postRun: Array<{ (mod: PostgresMod): void }>
  thisProgram: string
  stdin: (() => number | null) | null
  FS: FS
  wasmMemory: WebAssembly.Memory
  PROXYFS: Emscripten.FileSystemType
  WASM_PREFIX: string
  pg_extensions: Record<string, Promise<Blob | null>>
  UTF8ToString: (ptr: number, maxBytesToRead?: number) => string
  stringToUTF8OnStack: (s: string) => number
  _pgl_set_system_fn: (system_fn: number) => void
  _pgl_set_popen_fn: (popen_fn: number) => void
  _pgl_set_pclose_fn: (pclose_fn: number) => void
  _pgl_set_rw_cbs: (read_cb: number, write_cb: number) => void
  _pgl_set_pipe_fn: (pipe_fn: number) => number
  _pgl_freopen: (filepath: number, mode: number, stream: number) => number
  _pgl_pq_flush: () => void
  _fopen: (path: number, mode: number) => number
  _fclose: (stream: number) => number
  _fflush: (stream: number) => void
  _pgl_proc_exit: (code: number) => number
  addFunction: (
    cb: (ptr: any, length: number) => void,
    signature: string,
  ) => number
  removeFunction: (f: number) => void
  callMain: (args?: string[]) => number
  _PostgresMainLoopOnce: () => void
  _PostgresMainLongJmp: () => void
  _PostgresSendReadyForQueryIfNecessary: () => void
  _ProcessStartupPacket: (
    Port: number,
    ssl_done: boolean,
    gss_done: boolean,
  ) => number
  // althought the C function returns bool, we receive in JS a number
  _IsTransactionBlock: () => number
  _pgl_setPGliteActive: (newValue: number) => number
  // Sequence leases (postgres-pglite src/include/pglite.h, design §5.3):
  // int64 args cross the WASM boundary as BigInt (-sWASM_BIGINT).
  _pgl_set_sequence_lease: (seqOid: number, leaseEnd: bigint) => void
  _pgl_clear_sequence_leases: () => void
  _pgl_reset_sequence_caches: () => void
  // WAL-range scanner (postgres-pglite src/backend/pglite/pgl_walscan.c,
  // design §14.2): per-record JSON classification of a [start, end) LSN
  // range read straight from pg_wal segment files.
  _pgl_walscan_begin: (start: bigint, end: bigint, tli: number) => number
  _pgl_walscan_next: () => number
  _pgl_walscan_block_image: (blockId: number, dst: number) => number
  _pgl_walscan_end_scan: () => void
  // Live tail-apply primitives (pgl_apply.c, design §6.3/§5.1).
  _pgl_current_insert_lsn: () => bigint
  _pgl_process_invals: (
    msgsPtr: number,
    nmsgs: number,
    relcacheInitFileInval: boolean,
    dbId: number,
    tsId: number,
  ) => void
  _pgl_advance_identity: (
    nextFullXid: bigint,
    nextOid: number,
    nextMulti: number,
    nextOffset: number,
  ) => void
  _pgl_advance_xid_past: (xid: number) => void
  _pgl_clog_set: (xid: number, status: number) => void
  _pgl_clog_zero_page: (pageno: bigint) => void
  _pgl_multixact_zero_off_page: (pageno: bigint) => void
  _pgl_multixact_zero_mem_page: (pageno: bigint) => void
  _pgl_multixact_record: (
    mid: number,
    moff: number,
    nmembers: number,
    membersPtr: number,
  ) => void
  _pgl_invalidate_xact_caches: () => void
  _pgl_drop_relation_buffers_range: (
    spcOid: number,
    dbOid: number,
    relNumber: number,
    forkNum: number,
    firstBlock: number,
    blockCount: number,
  ) => void
  _pgl_smgr_release: (spcOid: number, dbOid: number, relNumber: number) => void
  _pgl_smgr_destroy_all: () => void
  // Read-set capture (pgl_readset.c, design §4.1) — machinery for the M5d
  // rebase validator. Entries are packed uint32 x5.
  _pgl_readset_enable: (on: number) => void
  _pgl_readset_reset: () => void
  _pgl_readset_count: () => number
  _pgl_readset_overflowed: () => number
  _pgl_readset_snapshot: () => number
  _malloc: (size: number) => number
  _free: (ptr: number) => void
  HEAPU8: Uint8Array
  _pgl_startPGlite: () => void
  _pgl_getMyProcPort: () => number
  _pgl_sendConnData: () => void
  ENV: any
  PGLITE_ENV: any
  _emscripten_force_exit: (status: number) => void
  _pgl_run_atexit_funcs: () => void
  _pq_buffer_remaining_data: () => number
}

type PostgresFactory<T extends PostgresMod = PostgresMod> = (
  moduleOverrides?: Partial<T>,
) => Promise<T>

export default PostgresModFactory as PostgresFactory<PostgresMod>
