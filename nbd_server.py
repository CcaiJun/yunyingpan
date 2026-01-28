#!/usr/bin/env python3
import socket
import struct
import threading
import logging
from block_manager import BlockManager

logger = logging.getLogger(__name__)

# NBD Constants
NBD_MAGIC = 0x4e42444d41474943
NBD_OPTS_MAGIC = 0x49484156454f5054
NBD_REP_MAGIC = 0x3e889045565a9

# Handshake flags
NBD_FLAG_FIXED_NEWSTYLE = 1 << 0
NBD_FLAG_NO_ZEROES = 1 << 1

# Transmission flags
NBD_FLAG_HAS_FLAGS = 1 << 0
NBD_FLAG_READ_ONLY = 1 << 1
NBD_FLAG_SEND_FLUSH = 1 << 2
NBD_FLAG_SEND_FUA = 1 << 3

# Options
NBD_OPT_EXPORT_NAME = 1
NBD_OPT_ABORT = 2
NBD_OPT_LIST = 3
NBD_OPT_STARTTLS = 5
NBD_OPT_INFO = 6
NBD_OPT_GO = 7

# Commands
NBD_CMD_READ = 0
NBD_CMD_WRITE = 1
NBD_CMD_DISC = 2
NBD_CMD_FLUSH = 3

# Reply types
NBD_REP_ACK = 1
NBD_REP_ERR_UNSUP = 2**31 + 1

class NBDServer:
    def __init__(self, bm: BlockManager, host='127.0.0.1', port=10810):
        self.bm = bm
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.running = False

    def start(self):
        self.sock.bind((self.host, self.port))
        self.sock.listen(5)
        self.running = True
        logger.info(f"NBD Server listening on {self.host}:{self.port}")
        while self.running:
            try:
                conn, addr = self.sock.accept()
                threading.Thread(target=self.handle_client, args=(conn,), daemon=True).start()
            except Exception as e:
                if self.running:
                    logger.error(f"Error accepting connection: {e}")

    def _recv_all(self, conn, n):
        data = b''
        while len(data) < n:
            packet = conn.recv(n - len(data))
            if not packet:
                return None
            data += packet
        return data

    def handle_client(self, conn):
        try:
            # 1. Handshake Phase
            # S: magic (NBDMAGIC), opts_magic (IHAVEOPT), handshake_flags
            conn.sendall(struct.pack(">QQH", NBD_MAGIC, NBD_OPTS_MAGIC, NBD_FLAG_FIXED_NEWSTYLE))
            
            # C: client flags
            data = self._recv_all(conn, 4)
            if data is None: return
            client_flags = struct.unpack(">I", data)[0]
            
            # Options loop
            while True:
                opt_header = self._recv_all(conn, 16)
                if opt_header is None: break
                magic, opt, length = struct.unpack(">QII", opt_header)
                if magic != NBD_OPTS_MAGIC:
                    logger.error(f"Invalid option magic: {hex(magic)}")
                    break
                
                # Option data
                data = self._recv_all(conn, length) if length > 0 else b''
                
                if opt == NBD_OPT_EXPORT_NAME:
                    # Transmission phase starts after this
                    transmission_flags = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH
                    conn.sendall(struct.pack(">QH", self.bm.disk_size, transmission_flags))
                    if not (client_flags & NBD_FLAG_NO_ZEROES):
                        conn.sendall(b'\x00' * 124)
                    break
                elif opt == NBD_OPT_GO or opt == NBD_OPT_INFO:
                    # nbd-client tries GO/INFO. We return ERR_UNSUP to force it fallback to EXPORT_NAME
                    # or implement GO properly. For now, let's try to just return UNSUP.
                    conn.sendall(struct.pack(">QIII", NBD_REP_MAGIC, opt, NBD_REP_ERR_UNSUP, 0))
                elif opt == NBD_OPT_ABORT:
                    conn.close()
                    return
                elif opt == NBD_OPT_LIST:
                    # Return a list of exports
                    export_name = b"default"
                    reply_len = 4 + len(export_name)
                    conn.sendall(struct.pack(">QIII", NBD_REP_MAGIC, opt, NBD_REP_SERVER, reply_len))
                    conn.sendall(struct.pack(">I", len(export_name)) + export_name)
                    # End list
                    conn.sendall(struct.pack(">QIII", NBD_REP_MAGIC, opt, NBD_REP_ACK, 0))
                else:
                    # Unsupported option
                    conn.sendall(struct.pack(">QIII", NBD_REP_MAGIC, opt, NBD_REP_ERR_UNSUP, 0))
            
            # 2. Transmission Phase
            while True:
                req_header = self._recv_all(conn, 28)
                if req_header is None: break
                magic, flags, type, handle, offset, length = struct.unpack(">IHHQQI", req_header)
                if magic != 0x25609513: break # NBD_REQUEST_MAGIC
                
                if type == NBD_CMD_READ:
                    data = self.bm.read(length, offset)
                    conn.sendall(struct.pack(">IIQ", 0x67446698, 0, handle) + data)
                elif type == NBD_CMD_WRITE:
                    data = self._recv_all(conn, length)
                    if data is None: break
                    self.bm.write(data, offset)
                    conn.sendall(struct.pack(">IIQ", 0x67446698, 0, handle))
                elif type == NBD_CMD_DISC:
                    break
                elif type == NBD_CMD_FLUSH:
                    self.bm.sync()
                    conn.sendall(struct.pack(">IIQ", 0x67446698, 0, handle))
                else:
                    # Unsupported command, send error
                    conn.sendall(struct.pack(">IIQ", 0x67446698, 1, handle)) # 1 is EPERM

        except Exception as e:
            logger.error(f"NBD client error: {e}")
        finally:
            conn.close()

def main_nbd(host, port, dav_url, dav_user, dav_password, cache_dir, disk_size_gb, max_cache_gb, block_size_mb, img_name, remote_path, concurrency):
    bm = BlockManager(dav_url, dav_user, dav_password, cache_dir, disk_size_gb, max_cache_gb, 
                     block_size_mb, img_name, remote_path, concurrency)
    server = NBDServer(bm, host, port)
    server.start()

if __name__ == '__main__':
    import sys
    # Simple CLI for testing
    if len(sys.argv) < 6:
        print("Usage: nbd_server.py <port> <dav_url> <dav_user> <dav_password> <cache_dir>")
        sys.exit(1)
    
    logging.basicConfig(level=logging.INFO)
    main_nbd('127.0.0.1', int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], 
             10, 2, 4, "nbd_disk", "blocks", 4)
