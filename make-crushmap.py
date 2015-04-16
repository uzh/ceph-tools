#! /usr/bin/env python

from copy import deepcopy
import os
import sys


def main():
    # read Crush Map
    m = CrushMap()
    m.parse(sys.stdin)

    # alter map as needed
    for name, data in m.hosts.items():
        weights = set(data['item'].values())

        if len(weights) == 2:
            # SSD drives are smaller, hence they have a smaller weight
            wt_ssd = min(weights)
            wt_hdd = max(weights)

            # create group of SSD OSDs
            ssd = deepcopy(data)
            ssd['id'] = m.new_id()
            for osd, wt in ssd['item'].items():
                if wt != wt_ssd:
                    del ssd['item'][osd]
            tot_wt_ssd = sum(wt for wt in ssd['item'].values())
            m.hosts[name + '-ssd'] = ssd

            # create group of HDD OSDs
            hdd = deepcopy(data)
            hdd['id'] = m.new_id()
            for osd, wt in hdd['item'].items():
                if wt != wt_hdd:
                    del hdd['item'][osd]
            tot_wt_hdd = sum(wt for wt in hdd['item'].values())
            m.hosts[name + '-hdd'] = hdd

            # now alter the original host to point to the two groups
            m.hosts[name]['item'] = {
                name + '-ssd': tot_wt_ssd,
                name + '-hdd': tot_wt_hdd,
            }

    # pretty-print map back to output
    m.pprint(sys.stdout)


class CrushMap(object):
    def __init__(self):
        self._ids = set()
        self.tunables = {}
        self.devices = {}
        self.types = {}
        self.hosts = {}
        self.roots = {}
        self.rules = {}

    def parse(self, stream):
        # do the actual parsing
        input = _PartsIterator(stream)
        for parts in input:
            if 'tunable' == parts[0]:
                self.tunables[parts[1]] = parts[2]
            elif 'device' == parts[0]:
                self.devices[parts[1]] = parts[2]
            elif 'type' == parts[0]:
                self.types[parts[1]] = parts[2]
            elif 'host' == parts[0]:
                self.hosts[parts[1]] = self._parse_host_or_root(parts[1], input)
            elif 'root' == parts[0]:
                self.roots[parts[1]] = self._parse_host_or_root(parts[1], input)
            elif 'rule' == parts[0]:
                self.rules[parts[1]] = self._parse_rule(parts[1], input)

    def _parse_host_or_root(self, name, input):
        data = { '#name':name, 'item':{} }
        for parts in input:
            if '}' == parts[0]:
                return data
            elif 'id' == parts[0]:
                id = int(parts[1])
                self._ids.add(id)
                data['id'] = id
            elif 'item' == parts[0]:
                data['item'][parts[1]] = float(parts[3])
            else:
                data[parts[0]] = parts[1]

    def _parse_rule(self, name, input):
        rules = { '#name':name, 'step':{} }
        for parts in input:
            if '}' == parts[0]:
                return rules
            elif 'step' == parts[0]:
                rules['step'][parts[1]] = str.join(' ', parts[2:])
            else:
                rules[parts[0]] = str.join(' ', parts[1:])

    def new_id(self):
        id = min(self._ids) - 1
        self._ids.add(id)
        return id

    def pprint(self, stream=sys.stdout):
        for k, v in sorted(self.tunables.items(), key=(lambda it: it[0])):
            stream.write("tunable %s %s\n" % (k, v))
        stream.write('\n')

        for k, v in sorted(self.devices.items(), key=(lambda it: int(it[0]))):
            stream.write("device %s %s\n" % (k, v))
        stream.write('\n')

        for k, v in sorted(self.types.items(), key=(lambda it: int(it[0]))):
            stream.write("device %s %s\n" % (k, v))
        stream.write('\n')

        for k, v in sorted(self.hosts.items(), key=(lambda it: -it[1]['id'])):
            self._pprint_host_or_root("host", stream, k, v)
            stream.write('\n')

        for k, v in sorted(self.roots.items(), key=(lambda it: -it[1]['id'])):
            self._pprint_host_or_root("root", stream, k, v)
            stream.write('\n')

        for k, v in sorted(self.rules.items(), key=(lambda it: it[0])):
            self._pprint_rule(stream, k, v)
            stream.write('\n')

    def _pprint_host_or_root(self, kind, stream, name, data):
        stream.write("%s %s {\n" % (kind, name))
        #stream.write("  id %d\n" % (data['id'],))
        for k, v in data.items():
            if 'item' == k:
                continue
            stream.write("  %s %s\n" % (k, v))
        for k, v in data['item'].items():
            stream.write('  item %s weight %s\n' % (k, v))
        stream.write('}\n')

    def _pprint_rule(self, stream, name, data):
        stream.write("rule %s {\n" % (name,))
        for k, v in data.items():
            if 'step' == k:
                continue
            stream.write("  %s %s\n" % (k, v))
        for k, v in data['step'].items():
            stream.write('  step %s %s\n' % (k, v))
        stream.write('}\n')


class _PartsIterator(object):
    def __init__(self, stream):
        self._stream = stream
    def __iter__(self):
        return self
    def next(self):
        # advance to next non-blank and non-comment line
        line = ''
        while line == '' or line.startswith('#'):
            line = self._stream.next()
            line = line.strip()
        parts = line.split()
        return parts


if __name__ == '__main__':
    main()
    #from pprint import pprint
    #for data in m.tunables, m.devices, m.types, m.hosts, m.roots, m.rules:
    #    pprint(data)
