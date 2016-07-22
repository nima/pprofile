import pprofile
import json
import re
import copy
from md5 import md5
from itertools import groupby

in_color = True
try:
    from pygments import highlight, lexers, formatters
except ImportError:
    in_color = False

def jprint(obj):
    formatted_json = json.dumps(obj, sort_keys=True, indent=2)

    if in_color:
        output = highlight(
            unicode(formatted_json, 'UTF-8'),
            lexers.JsonLexer(),
            formatters.TerminalFormatter()
        )
    else:
        output = formatted_json

    print(output)

class Profiler:
    @staticmethod
    def filename(tag, fn, *args, **kwargs):
        hsh = md5(
            ', '.join(
                [
                    '%s' % a for a in args
                ] + [
                    '%s=%s' % (k, v) for (k, v) in kwargs.items()
                ]
            )
        ).hexdigest()

        return '/tmp/pprofiler-save-%s-%s(%s).json' % (tag, fn.__name__, hsh)

    @staticmethod
    def profile(tag, fn, *args, **kwargs):
        '''Returns the expensive stats object for consumption in the `Profiler'.'''

        saved = Profiler.filename(tag, fn, *args, **kwargs)
        try:
            with open(saved, 'r') as fH:
                dump = json.load(fH)
        except IOError:
            profiler = pprofile.Profile()
            with profiler:
                fn(*args, **kwargs)
            dump = profiler.get_stats()

            with open(saved, 'w') as fH:
                fH.write(json.dumps(dump))

        return dump


    @staticmethod
    def _colorize_code(lines):
        color_lines = copy.deepcopy(lines)

        if in_color:
            color_lines = highlight(
                lines, lexers.PythonLexer(), formatters.TerminalFormatter()
            ).split('\n')

            while len(color_lines) < len(lines):
                color_lines.append('\n')

        return color_lines

    def __init__(self, stats):
        self._stats = stats

    def overhead(self):
        actual = sum([f['duration'] for f in self._stats['command_profile']])
        return {
            'total': self._stats['duration'],
            'actual': actual,
            'overhead': self._stats['duration'] - actual,
        }

    def get_partitioned_stats(self):
        buckets = {}
        for file_profile in self._stats['command_profile']:
            duration = file_profile['duration']
            fraction = duration / self._stats['duration']
            bucket = int(('%E' % duration).split('E')[-1])
            details = {}

            for line_profile in file_profile['file_profile']:
                duration = line_profile['duration']
                subbucket = int(('%E' % duration).split('E')[-1])
                details.setdefault(subbucket, []).append(line_profile)

            buckets.setdefault(bucket, []).append({
                'file_name': file_profile['file_name'],
                'duration': file_profile['duration'],
                'details': details,
            })

        return { 10**k: v for (k,v) in buckets.items() }

    def _get_stats_by_loc(self, filters=[]):
        flat = {}

        for file_profile in self._stats['command_profile']:
            for line_profile in file_profile['file_profile']:
                hit = True
                for ftype, fregex in filters:
                    if ftype == 'file':
                        hit = bool(fregex.match(file_profile['file_name']))
                    elif ftype == 'line':
                        hit = bool(fregex.match(line_profile['line']))
                    else:
                        raise RuntimeError("<ftype> must be one of `file' or `line'")

                    if hit:
                        break

                if hit:
                    key = '%s:%s' % (
                        file_profile['file_name'],
                        line_profile['line_no']
                    )
                    flat[key] = {
                        'duration': file_profile['duration'],
                        'file_name': file_profile['file_name'],
                        'line': line_profile,
                    }

        return flat

    def get_filtered_profile(self, t, filters=[]):
        filters = [
            (ftype, re.compile(fstr)) for (ftype, fstr) in [
                f.split(':', 1) for f in filters
            ]
        ]

        return sorted(
            [
                {
                    'address': l,
                    'datum': d,
                } for (l, d) in self._get_stats_by_loc(filters).items()
                    if d['line']['duration'] >= t
            ], key=lambda stats: stats['datum']['line']['duration'], reverse=True
        )

    def _resolve_filedesc(self, filedesc):
        file_profiles = [
            file_profile for file_profile in self._stats['command_profile']
        ]

        if type(filedesc) is int:
            index = filedesc
        else:
            file_path = filedesc
            try:
                index = [fp['file_name'] for fp in file_profiles].index(file_path)
            except ValueError:
                raise RuntimeError("No such file `%s'" % file_path)

        file_profile = copy.deepcopy(self._stats['command_profile'][index])

        return (index, file_profile)

    def get_dump(self, filedesc, block_mode_too=False, t=0):
        _, dump = self._resolve_filedesc(filedesc)

        if block_mode_too:
            file_profile_by_block = {}
            for k, v in groupby(
                dump['file_profile'], lambda l: ':'.join(
                    [str(bid) for bid in l['block_id']]
                )
            ):
                file_profile_by_block.setdefault(k, []).append(list(v))

            #for k, v in file_profile_by_block.items():
            #    file_profile_by_block[
            dump['file_profile'] = file_profile_by_block

        return dump

    def dump(self, filedesc, block_mode=False, t=0):
        jprint(self.get_dump(filedesc, block_mode, t))

    def cat(self, filedesc, ln_from=0, ln_to=-1):
        index, file_profile = self._resolve_filedesc(filedesc)
        color_lines = Profiler._colorize_code('\n'.join([
            line['line'] for line in file_profile['file_profile']
        ]))

        i = 0
        for line in file_profile['file_profile']:
            if i >= (ln_from-1) and ( ln_to < 0 or i <= (ln_to-1) ):
                print(
                    "%5.1f/%-9d | %04d | %s" % (
                        line['duration'],
                        line['hits'],
                        line['line_no'],
                        color_lines and color_lines[i] or line['line']
                    )
                )
            i += 1

    def ls(self, t=0):
        i = 0
        for file_profile in self._stats['command_profile']:
            if file_profile['duration'] >= t:
                print("%3d | %6.1f | %s" % (
                    i,
                    file_profile['duration'],
                    file_profile['file_name']
                ))
            else:
                break
            i += 1

    def grep(self, regex, filedesc=None):
        file_profiles = []
        index = 0
        if filedesc is not None:
            index, file_profile = self._resolve_filedesc(filedesc)
            file_profiles.append((index, file_profile))
        else:
            for fp in self._stats['command_profile']:
                file_profiles.append((index, fp))
                index += 1

        regex = re.compile(regex)

        for (i, file_profile) in file_profiles:
            headed = False
            color_lines = Profiler._colorize_code('\n'.join([
                line['line'] for line in file_profile['file_profile']
            ]))

            j = 0
            for line in file_profile['file_profile']:
                if regex.match(line['line']):
                    if not headed:
                        filepath = file_profile['file_name']
                        print("# file[%d]: %s" % (i, filepath))
                        headed = True
                    print(
                        "%5.1f/%-9d | %04d | %s" % (
                            line['duration'],
                            line['hits'],
                            line['line_no'],
                            color_lines and color_lines[j] or line['line']
                        )
                    )
                j += 1
