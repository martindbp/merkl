import unittest
from merkl import *


class TestIO(unittest.TestCase):
    def test_caching(self):
        task_has_run = False

        @task(caches=[InMemoryCache])
        def task1(arg):
            nonlocal task_has_run
            task_has_run = True
            return arg

        val = task1('test').eval()
        self.assertEqual(val, 'test')
        # Make sure task_has_run was changed
        self.assertTrue(task_has_run)

        # Now reset and test that value is cached (task_has_run is not changed)
        task_has_run = False
        val = task1('test').eval()
        self.assertEqual(val, 'test')
        self.assertFalse(task_has_run)


if __name__ == '__main__':
    unittest.main()
