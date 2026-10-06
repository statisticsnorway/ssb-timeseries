from pathlib import Path

BRANCHES = 10
NODE_CAP = 4

def tree(start_in_dir, branches=BRANCHES, node_cap=NODE_CAP):
    """Render a directory tree capped at `branches` top-level entries.

    Everything below the top level is capped per parent at `node_cap`, split into a first and a last run around a marker.
    Depth is not bounded, so a deep and dense tree can still grow as branches * node_cap ** depth, and the size check in tools/export_all_guides.py is the backstop.
    A `max_depth` parameter, and possibly a total width limit, would bound that here if it is ever needed.
    """
    out = ''
    path = Path(start_in_dir)
    if path.is_dir():
        paths = DisplayablePath.make_tree(
            path,
            criteria=is_not_hidden,
            branches=branches,
            node_cap=node_cap
        )
        for p in paths:
            out += p.displayable() +'\n'

    return out

def _cap_first(items, limit):
    """Keep the first `limit` items and report whether any were cut."""
    if len(items) <= limit:
        return items, False
    return items[:max(limit, 0)], True

def _cap_middle(items, limit):
    """Keep `limit` items split into a first and a last run around a marker."""
    if len(items) <= limit:
        return items, False
    if limit < 2:
        return _cap_first(items, limit)
    first = limit // 2
    last = limit - first
    return items[:first] + items[len(items) - last:], True

class DisplayablePath(object):
    display_filename_prefix_middle = '├──'
    display_filename_prefix_last = '└──'
    display_parent_prefix_middle = '    '
    display_parent_prefix_last = '│   '

    def __init__(self, path, parent_path, is_last, is_marker=False):
        self.path = Path(str(path))
        self.parent = parent_path
        self.is_last = is_last
        self.is_marker = is_marker
        if self.parent:
            self.depth = self.parent.depth + 1
        else:
            self.depth = 0

    @property
    def displayname(self):
        if self.is_marker:
            return '...'
        if self.path.is_dir():
            return self.path.name + '/'
        return self.path.name

    @classmethod
    def make_tree(cls, root, parent=None, is_last=False, criteria=None,
                  branches=BRANCHES, node_cap=NODE_CAP):
        root = Path(str(root))
        criteria = criteria or cls._default_criteria

        displayable_root = cls(root, parent, is_last)
        yield displayable_root

        children = sorted(list(path
                               for path in root.iterdir()
                               if criteria(path)),
                          key=lambda s: str(s).lower())

        if displayable_root.depth == 0:
            shown, truncated = _cap_first(children, branches)
        else:
            shown, truncated = _cap_middle(children, node_cap)
        if truncated:
            shown = shown + [None]

        for index, child in enumerate(shown):
            is_last = index == len(shown) - 1
            if child is None:
                yield cls(Path('...'), displayable_root, is_last,
                          is_marker=True)
            elif child.is_dir():
                yield from cls.make_tree(child,
                                         parent=displayable_root,
                                         is_last=is_last,
                                         criteria=criteria,
                                         branches=branches,
                                         node_cap=node_cap)
            else:
                yield cls(child, displayable_root, is_last)

    @classmethod
    def _default_criteria(cls, path):
        return True

    def displayable(self):
        if self.parent is None:
            return self.displayname

        _filename_prefix = (self.display_filename_prefix_last
                            if self.is_last
                            else self.display_filename_prefix_middle)

        parts = ['{!s} {!s}'.format(_filename_prefix,
                                    self.displayname)]

        parent = self.parent
        while parent and parent.parent is not None:
            parts.append(self.display_parent_prefix_middle
                          if parent.is_last
                          else self.display_parent_prefix_last)
            parent = parent.parent

        return ''.join(reversed(parts))

def is_not_hidden(path):
    return not path.name.startswith(".")
