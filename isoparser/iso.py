from . import susp, rockridge

from . import source as Source


class ISO(object):
    def __init__(self, source, joliet=True):
        self._source = source

        # Unpack volume descriptors
        self.volume_descriptors = {}
        self.joliet = joliet
        sector = 16
        while True:
            self._source.seek(sector)

            try:
                vd = self._source.unpack_volume_descriptor()
            except Source.SourceError as e:
                # Attempt MODE1/2352 BIN
                self._source.reinit(sector_start = 16, sector_length = 2352)
                self._source.seek(sector)
                vd = self._source.unpack_volume_descriptor()

            sector += 1

            self.volume_descriptors[vd.name] = vd

            if vd.name == "terminator":
                break

        path_descriptor = "primary"

        # Unpack the Joliet path table (if supplementary exists)
        if joliet and 'supplementary' in self.volume_descriptors:
          path_descriptor = "supplementary"

        # Unpack the path table
        self._source.seek(
            self.volume_descriptors[path_descriptor].path_table_l_loc,
            self.volume_descriptors[path_descriptor].path_table_size)
        self.path_table = self._source.unpack_path_table(path_descriptor)

        # Save a reference to the root record
        self.root = self.volume_descriptors[path_descriptor].root_record

        # Check to see if SUSP is enabled
        root_record = self.root.current_directory
        if root_record.embedded_susp_entries and isinstance(root_record.embedded_susp_entries[0], susp.SP):
            self._source.susp_starting_index = root_record.embedded_susp_entries[0].len_skp
            self._source.susp_extensions = [e for e in root_record.susp_entries if isinstance(e, susp.ER)]
            if any(((er.ext_id, er.ext_ver) in rockridge.EXT_VERSIONS) for er in self._source.susp_extensions):
                self._source.rockridge = True
        else:
            self._source.susp_starting_index = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._source.close()

    def record(self, *path):
        """
        Retrieves a record for the given path.
        """
        record = None
        if self._source.rockridge:
            # In Rock Ridge mode, we can't use the path table
            pivot = 0
        else:
            if 'supplementary' in self.volume_descriptors:
                # Joliet
                path = [part.decode('utf-8').encode('utf-16be') for part in path]
                pivot = len(path)
                pass
            else:
                path = [part.upper() for part in path]
                pivot = len(path)


        # Resolve as much of the path as possible via the path table
        while pivot > 0:
            try:
                record = self.path_table.record(*path[:pivot])
            except KeyError:
                pivot -= 1
            else:
                break

        if record is None:
            record = self.root

        # Resolve the remainder of the path by walking record children
        for part in path[pivot:]:
            for child in record.children_unsafe:
                # Must save the cursor since child.name can cause a seek
                saved_cursor = self._source.save_cursor()
                if child.name == part:
                    record = child
                    break
                self._source.restore_cursor(saved_cursor)
            else:
                raise KeyError(part)

        return record
