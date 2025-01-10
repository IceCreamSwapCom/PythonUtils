import unittest
from versioned_dict import VersionedDict

class TestVersionedDict(unittest.TestCase):
    def test_basic_operations(self):
        vd = VersionedDict()

        # Test set and get
        vd['key1'] = 'value1'
        self.assertEqual(vd['key1'], 'value1')

        # Test delete
        del vd['key1']
        self.assertNotIn('key1', vd)

    def test_commit_and_rollback(self):
        vd = VersionedDict(max_history=3)

        # Add initial state
        vd['key1'] = 'value1'
        vd.commit()

        # Make changes
        vd['key2'] = 'value2'
        vd['key1'] = 'new_value'
        vd.commit()

        # More changes
        vd['key3'] = 'value3'

        # Rollback the last change
        changes = vd.revert()
        self.assertEqual(changes, ('key3',))
        self.assertNotIn('key3', vd)
        self.assertEqual(vd['key1'], 'new_value')
        self.assertEqual(vd['key2'], 'value2')

        # Rollback to previous state
        changes = vd.rollback()
        self.assertEqual(set(changes), {'key1', 'key2'})
        self.assertNotIn('key2', vd)
        self.assertEqual(vd['key1'], 'value1')

    def test_rollback_empty_history(self):
        vd = VersionedDict()
        vd['key1'] = 'value1'
        vd.commit()
        vd.rollback()

        with self.assertRaises(ValueError):
            vd.rollback()  # No more history to rollback

    def test_immutable_values_only(self):
        vd = VersionedDict()

        # Immutable values are allowed
        vd['key1'] = 'value1'
        vd['key2'] = 42
        vd['key3'] = (1, 2, 3)

        # Mutable values should raise ValueError
        with self.assertRaises(ValueError):
            vd['key4'] = [1, 2, 3]

    def test_thread_safety(self):
        import threading
        import random
        from collections import deque

        vd = VersionedDict()

        def random_operations(thread_id):
            for _ in range(1000):
                op = random.choice(['set', 'get', 'delete', 'commit', 'rollback'])
                key = f'key{random.randint(0, 100)}'
                if op == 'set':
                    vd[key] = f'value_from_thread_{thread_id}'
                elif op == 'get':
                    _ = vd.get(key, None)
                elif op == 'delete':
                    vd.pop(key, None)
                elif op == 'commit':
                    vd.commit()
                elif op == 'rollback':
                    try:
                        vd.rollback()
                    except ValueError:
                        pass

        threads = [threading.Thread(target=random_operations, args=(i,)) for i in range(10)]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Verify internal state integrity
        self.assertTrue(isinstance(vd._changes, dict))
        self.assertTrue(isinstance(vd._history, deque))

    def test_max_history_limit(self):
        vd = VersionedDict(max_history=2)

        vd['key1'] = 'value1'
        vd.commit()
        vd['key2'] = 'value2'
        vd.commit()
        vd['key3'] = 'value3'
        vd.commit()

        # Only the last two commits should be kept
        changes = vd.rollback()  # Removes key3
        self.assertEqual(changes, ('key3',))
        self.assertEqual(vd['key2'], 'value2')
        changes = vd.rollback()  # Removes key2
        self.assertEqual(changes, ('key2',))
        self.assertNotIn('key2', vd)
        self.assertNotIn('key3', vd)

        with self.assertRaises(ValueError):
            vd.rollback()  # History is empty

    def test_to_json_and_from_json(self):
        vd = VersionedDict(max_history=2)

        vd['key1'] = 'value1'
        vd.commit()
        vd['key2'] = 'value2'
        vd.commit()

        # Serialize to JSON
        as_obj = vd.to_obj()

        # Deserialize from JSON
        restored_vd = VersionedDict.from_obj(as_obj)

        # Check restored object matches the original
        self.assertEqual(restored_vd.data, vd.data)
        self.assertEqual(restored_vd._history, vd._history)
        self.assertEqual(restored_vd._changes, vd._changes)
        self.assertEqual(restored_vd._history.maxlen, vd._history.maxlen)

        # Further changes to ensure restored object works as expected
        restored_vd['key3'] = 'value3'
        restored_vd.commit()
        changes = restored_vd.rollback()
        self.assertEqual(changes, ('key3',))
        self.assertNotIn('key3', restored_vd)

    def test_rollback_return_value(self):
        vd = VersionedDict(max_history=3)

        vd['key1'] = 'value1'
        vd.commit()
        vd['key1'] = 'new_value'
        vd['key2'] = 'value2'

        changes = vd.revert()
        self.assertEqual(set(changes), {'key1', 'key2'})
        self.assertEqual(vd['key1'], 'value1')
        self.assertNotIn('key2', vd)

if __name__ == '__main__':
    unittest.main()
