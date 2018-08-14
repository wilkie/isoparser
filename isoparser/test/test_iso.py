#! /usr/bin/env python
import unittest
import isoparser

from isoparser.test.test_data import TEST_DATA


class TestIso(unittest.TestCase):
    def recursive_test_record(self, record, content):
        self.assertTrue(record.is_directory)
        self.assertEqual(len(record.children), len(content))

        for child in record.children:
            self.assertIn(child.name, content)
            value = content[child.name]
            if child.is_directory:
                self.recursive_test_record(child, value)
            else:
                self.assertEqual(child.content, value)

    def test_root(self):
        for filename, content in TEST_DATA:
            iso = isoparser.parse(filename, joliet=False)
            self.assertEqual(len(iso.root.children), len(content))
            self.recursive_test_record(iso.root, content)
            iso.close()

    def test_root_joliet(self):
        for filename, content in TEST_DATA:
            # Rebuild content with utf-16be data (for Joliet schemes)
            def reencode(content):
                ret = {}
                for k,v in content.items():
                    # When characters aren't allowed, they are replaced with _
                    if b'?' in k:
                      k = k.replace(b"?", b"_")
                    if b';' in k:
                      k = k.replace(b";", b"_")
                    if b'\\' in k:
                      k = k.replace(b"\\", b"_")

                    new_name = k.decode('utf-8').encode('utf-16be')
                    if len(new_name) > 128:
                        # It will also truncate whitespace
                        new_name = new_name[0:128]
                        new_name = new_name.decode('utf-16be').rstrip().encode('utf-16be')

                    if isinstance(v, dict):
                        v = reencode(v)
                        
                    ret[new_name] = v

                return ret

            ucs2_content = reencode(content)

            iso = isoparser.parse(filename)
            self.assertEqual(len(iso.root.children), len(ucs2_content))
            self.recursive_test_record(iso.root, ucs2_content)
            iso.close()
