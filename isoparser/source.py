import datetime
import struct

from six.moves.urllib import request
from six.moves import range

from . import path_table, record, volume_descriptors, susp


SECTOR_LENGTH=2048


class SourceError(Exception):
    pass


class Source(object):
    def __init__(self, cache_content=False, min_fetch=16):
        self.cache_content = cache_content
        self.min_fetch = min_fetch
        self.reinit(0, SECTOR_LENGTH)

    def __len__(self):
        return len(self._buff) - self.cursor

    def reinit(self, sector_start, sector_length):
        self._buff = None
        self._sectors = {}
        self.cursor = None
        self.susp_starting_index = None
        self.susp_extensions = []
        self.rockridge = False
        self.sector_length = sector_length
        self.sector_start = sector_start

    def rewind_raw(self, l):
        if self.cursor < l:
            raise SourceError("Rewind buffer under-run")
        self.cursor -= l

    def unpack_raw(self, l):
        if l > len(self):
            raise SourceError("Source buffer under-run")
        data = self._buff[self.cursor:self.cursor + l]
        self.cursor += l
        return data

    def unpack_all(self):
        return self.unpack_raw(len(self))

    def unpack_boundary(self):
        return self.unpack_raw(SECTOR_LENGTH - (self.cursor % SECTOR_LENGTH))

    def unpack_both(self, st):
        a = self.unpack('<'+st)
        b = self.unpack('>'+st)
        if a != b:
            raise SourceError("Both-endian value mismatch")
        return a

    def unpack_string(self, l):
        return self.unpack_raw(l).rstrip(b' ')

    def unpack(self, st):
        if st[0] not in '<>':
            st = '<' + st
        d = struct.unpack(st, self.unpack_raw(struct.calcsize(st)))
        if len(st) == 2:
            return d[0]
        else:
            return d

    def rewind(self, st):
        self.rewind_raw(struct.calcsize(st))

    def unpack_vd_datetime(self):
        return self.unpack_raw(17)  # TODO

    def unpack_dir_datetime(self):
        epoch = datetime.datetime(1970, 1, 1)
        date = self.unpack_raw(7)
        t = [struct.unpack('<B', bytes([i]) if isinstance(i, int) else i)[0]
             for i in date]
        t.append(struct.unpack('<b', date[-1:])[0])
        t[0] += 1900
        t_offset = t.pop(-1) * 15 * 60.    # Offset from GMT in 15min intervals, converted to secs
        t_timestamp = (datetime.datetime(*t) - epoch).total_seconds() - t_offset
        t_datetime = datetime.datetime.fromtimestamp(t_timestamp)
        t_readable = t_datetime.strftime('%Y-%m-%d %H:%M:%S')
        return t_readable

    def unpack_volume_descriptor(self):
        ty = self.unpack('B')
        identifier = self.unpack_string(5)
        version = self.unpack('B')

        if identifier != b"CD001":
            raise SourceError("Wrong volume descriptor identifier")
        if version != 1:
            raise SourceError("Wrong volume descriptor version")

        if ty == 0:
            vd = volume_descriptors.BootVD(self)
        elif ty == 1:
            vd = volume_descriptors.PrimaryVD(self)
        elif ty == 2:
            vd = volume_descriptors.SupplementaryVD(self)
        elif ty == 3:
            vd = volume_descriptors.PartitionVD(self)
        elif ty == 255:
            vd = volume_descriptors.TerminatorVD(self)
        else:
            raise SourceError("Unknown volume descriptor type: %d" % ty)
        return vd

    def unpack_path_table(self, volume_descriptor_name):
        return path_table.PathTable(self, volume_descriptor_name)

    def unpack_record(self, volume_descriptor_name):
        start_cursor = self.cursor
        length = self.unpack('B')
        if length == 0:
            self.rewind('B')
            return None
        new_record = record.Record(self, length-1, self.susp_starting_index, volume_descriptor_name)
        assert self.cursor == start_cursor + length
        return new_record

    def unpack_susp(self, maxlen, possible_extension=0):
        if maxlen < 4:
            return None
        start_cursor = self.cursor
        signature = self.unpack_raw(2).decode()
        length = self.unpack('B')
        version = self.unpack('B')
        if maxlen < length:
            self.rewind_raw(4)
            return None
        if possible_extension < len(self.susp_extensions):
            extension = self.susp_extensions[possible_extension]
            ext_id_ver = (extension.ext_id, extension.ext_ver)
        else:
            ext_id_ver = None
        try:
            new_susp = susp.SUSP_Entry.unpack(self, ext_id_ver, (signature, version), length - 4)
        except susp.SUSPError:
            self.cursor = start_cursor
            # Fall into the next if statement
        if self.cursor != start_cursor + length:
            self.cursor = start_cursor + 4
            new_susp = susp.UnknownEntry(self, ext_id_ver, (signature, version), length - 4)
        assert self.cursor == start_cursor + length
        return new_susp

    def seek(self, start_sector, length=SECTOR_LENGTH, is_content=False):
        self.cursor = 0
        self._buff = b""
        do_caching = (not is_content or self.cache_content)
        n_sectors = 1 + (length - 1) // SECTOR_LENGTH
        fetch_sectors = max(self.min_fetch, n_sectors) if do_caching else n_sectors
        need_start = None

        def fetch_needed(need_count):
            data = self._fetch(need_start, need_count)

            if self.sector_length == 2048:
                self._buff += data
            else:
                # Extract sectors (for raw BIN/CUE)
                for sector_idx in range(need_count):
                    sector_data = data[self.sector_start + sector_idx*self.sector_length:self.sector_start + sector_idx*self.sector_length + 2048]
                    self._buff += sector_data

            if do_caching:
                for sector_idx in range(need_count):
                    sector_data = data[self.sector_start + sector_idx*self.sector_length:self.sector_start + sector_idx*self.sector_length + 2048]
                    self._sectors[need_start + sector_idx] = sector_data

        for sector in range(start_sector, start_sector + fetch_sectors):
            if sector in self._sectors:
                if need_start is not None:
                    fetch_needed(sector - need_start)
                    need_start = None
                # If we've gotten past the sectors we actually need, don't continue to fetch
                if sector >= start_sector + n_sectors:
                    break
                self._buff += self._sectors[sector]
            elif need_start is None:
                need_start = sector

        if need_start is not None:
            fetch_needed(start_sector + fetch_sectors - need_start)

        self._buff = self._buff[:length]

    def save_cursor(self):
        return (self._buff, self.cursor)

    def restore_cursor(self, cursor_def):
        self._buff, self.cursor = cursor_def

    def _fetch(self, sector, count=1):
        raise NotImplementedError

    def get_stream(self, sector, length):
        raise NotImplementedError

    def close(self):
        pass


class FileStream(Source):
    def __init__(self, file, offset, length, sector_start=0, sector_length=SECTOR_LENGTH):
        super(FileStream, self).__init__()
        self._file = file
        self._offset = offset
        self._length = length

        # The byte position in logical bytes
        self.position = 0

        # The byte position in physical bytes
        self.cur_offset = 0

        # The sector breakdown
        self.sector_start  = sector_start
        self.sector_length = sector_length

    def seek(self, new_position = 0, *args):
        """ Moves the pointer within the file stream to the specified byte.

        This moves the position relative to the file internally. So, if it
        is 0, it is either the start of the ISO/BIN or the start of the
        individual record.

        It takes into account the sector offset and length, which is 0 and
        2048 for a normal ISO, but different for a variety of raw CD binary
        dumps (most commonly 16 and 2352) and sets the internal cursor
        position accordingly.

        Args:
            new_position (int): The byte position within in the ISO or file.
        """

        if new_position > self._length:
            new_position = self._length

        self.position = new_position
        self.cur_offset = (self.sector_length * (new_position // 2048)) + (new_position % 2048)

    def read(self, size = -1, *args):
        """ Reads data from the ISO or record at its current location.

        Args:
            size (int): The number of bytes to read.

        Returns:
            bytes: The read bytes or an empty bytestring when nothing could be read.
        """

        if size < 0 or size > self._length - self.position:
            size = self._length - self.position
        self._file.seek(self.sector_start + self._offset + self.cur_offset)
        if self.sector_length == 2048:
            data = self._file.read(size)
            if data:
                self.cur_offset += len(data)
                self.position   += len(data)
        else:
            left = size
            within_sector = self.cur_offset % self.sector_length
            sector_remaining = 2048 - within_sector
            data = b''
            while left > 0:
                # Determine how much we can read within the sector
                amount = min(left, sector_remaining)

                # If we loop, we are always trying to read from the sector start
                sector_remaining = 2048

                # Get the data (within the sector our cursor is at)
                sector_data = self._file.read(amount)

                # Give up if we can't read the data
                if not sector_data:
                    break

                # Append the data
                data += sector_data

                # Advance our cursor within this sector
                self.cur_offset += len(sector_data)
                self.position   += len(sector_data)

                # If we have read a sector, skip the ECC/metdata and go to the
                # next sector (We cannot read over the sector boundary, so when
                # we consume a sector, our cursor is *at* the sector's data
                # boundary)
                if self.cur_offset % self.sector_length == 2048:
                  self.cur_offset += 304

                # Calculate how much data we have left to read
                left = size - len(data)

                if left > 0:
                  # Seek to the next position
                  self._file.seek(self.sector_start + self._offset + self.cur_offset)
        return data

    def _fetch(self, sector, count=1):
        self._file.seek(sector*self.sector_length)
        return self._file.read(self.sector_length*count)

    def close(self):
        pass


class FileSource(Source):
    def __init__(self, path, **kwargs):
        super(FileSource, self).__init__(**kwargs)
        self._file = open(path, 'rb')

    def _fetch(self, sector, count=1):
        self._file.seek(sector*self.sector_length)
        return self._file.read(self.sector_length*count)

    def get_stream(self, sector, length):
        return FileStream(self._file, sector*self.sector_length, length, self.sector_start, self.sector_length)

    def close(self):
        self._file.close()


class HTTPSource(Source):
    def __init__(self, url, **kwargs):
        super(HTTPSource, self).__init__(**kwargs)
        self._url = url

    def _fetch(self, sector, count=1):
        return self.get_stream(sector, count*self.sector_length).read()

    def get_stream(self, sector, length):
        opener = request.FancyURLopener()
        opener.http_error_206 = lambda *a, **k: None
        opener.addheader("Range", "bytes=%d-%d" % (
            self.sector_length * sector,
            self.sector_length * sector + length - 1))
        return opener.open(self._url)
