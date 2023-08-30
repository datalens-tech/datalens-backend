
import re

from .util import compat


class DatabaseException(Exception):

    code = None
    exc_type_name = None
    exc_message = None
    exc_message_extended = None
    server_version = None

    def __init__(self, orig):
        self.orig = orig
        self.maybe_parse_orig(orig)
        super(DatabaseException, self).__init__(orig)

    def maybe_parse_orig(self, orig):
        # Wrapped in case `str(orig)` is broken, primarily.
        # More generally, the parsing is best-effort here,
        # and shouldn't break the use-cases that don't rely on it.
        try:
            self.parse_orig(orig)
        except Exception:
            pass

    exc_text_re = (
        # re.IGNORECASE | re.DOTALL; *not* re.MULTILINE
        r'(?is)'
        r'^'
        r'(std::exception\.\s+)?'
        r'Code:\s+(?P<code>\d+)'
        # Everything but code is optional:
        r'(?:, type: (?P<exc_type_name>[^\s]+))?'
        r'(?:'  # exc_message + exc_message_extended
        r', e\.[^\s]+ = (?:DB::Exception: )?(?P<exc_message>[^\n]+)'
        r'(?:\n(?P<exc_message_extended>.*?))?'
        r')?'
    )
    exc_version_regexes = (
        (r'(?P<base>.*) \(version '
         r'(?P<version>[0-9][^\s]+)'
         r'(?: \(official build\))?'
         r'\)\s*$'),
        r'(?P<base>.*), version = (?P<version>[^\s]+)\s*$',
    )

    def _pick_out_version(self, text):
        if not text:
            return text, None
        for rex in self.exc_version_regexes:
            match = re.search(rex, text)
            if match:
                data = match.groupdict()
                return data['base'], data['version']
        return text, None

    def parse_orig(self, orig):
        text = str(orig)
        match = re.search(self.exc_text_re, text)
        if match:
            data = match.groupdict()
            self.code = data['code']
            self.exc_type_name = data['exc_type_name']
            exc_message, version_a = self._pick_out_version(
                data['exc_message'])
            exc_message_extended, version_b = self._pick_out_version(
                data['exc_message_extended'])

            self.exc_message = exc_message
            self.exc_message_extended = exc_message_extended
            self.server_version = version_a or version_b

    def __str__(self):
        text = 'Orig exception: {}'.format(self.orig)

        if compat.PY3:
            return compat.text_type(text)
        else:
            return compat.text_type(text).encode('utf-8')


if __name__ == '__main__':
    pass
