# Support for reading progsnap datasets.
# Copyright (c) 2016, David Hovemeyer <david.hovemeyer@gmail.com>

# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# See: http://cloudcoderdotorg.github.io/progsnap-spec/

import io
import json
import os
import re
import zipfile
import datetime
import functools

from numbers import Number

########################################################################
# Utility functions
########################################################################

# Scan file f, parsing each line as a standard progsnap
# data line (JSON object with tag and value fields),
# using the specified TagHandler object to process
# the lines.
def _scan(f, tag_handler):
    line_no = 0
    for line in f:
        line_no += 1

        # Reading a file from a zipfile yields bytes rather than str,
        # so convert if necessary.
        if type(line) is bytes:
            line = line.decode("utf-8")

        # Read as JSON object
        obj = json.loads(line)

        # Make sure the required fields are present
        if ('tag' not in obj) or ('value' not in obj) or (type(obj['tag']) is not str):
            raise ProgsnapError("Invalid data at line {}".format(line_no))

        # Find appropriate callback and invoke it
        tv_callback = tag_handler.get_callback(obj['tag'])
        tv_callback(obj['tag'], obj['value'])

# Get the directory part of a filename.
def _get_dirpart(fname):
    last_slash = fname.rfind('/')
    return fname[0:last_slash] if last_slash > 0 else ""

_isintre = re.compile(r"^\d+$")

def _isint(s):
    return _isintre.search(s)

_ishistfilere = re.compile(r"^\d+\.txt$")

def _ishistfile(s):
    return _ishistfilere.search(s)

_extractintre = re.compile(r"^(\d+)")

def _extractint(s):
    m = _extractintre.search(s)
    if not m:
        raise ProgsnapError("No integer in {}".format(s))
    return int(m.group(1))

########################################################################
# Classes
########################################################################

# Exception class for all fatal errors
class ProgsnapError(Exception):
    pass

# Abstraction for accessing progsnap data in a directory
class _ProgsnapDirectory():
    def __init__(self, basedir):
        self._basedir = basedir

    def open(self, filename):
        return open("{}/{}".format(self._basedir, filename))

    def isdir(self, path):
        return os.path.isdir("{}/{}".format(self._basedir, path))

    def isfile(self, path):
        return os.path.isfile("{}/{}".format(self._basedir, path))

    def listdir(self, path):
        return os.listdir("{}/{}".format(self._basedir, path))

# Abstraction for accessing progsnap data in a zipfile
class _ProgsnapZipfile():
    def __init__(self, path):
        self._zipfile = zipfile.ZipFile(path)
        self._dirs = set()
        self._find_dirs()

    # Find all of the names in the zipfile that represent
    # directories
    def _find_dirs(self):
        for info in self._zipfile.infolist():
            fname = info.filename

            # Trim trailing slash if necessary
            if fname.endswith('/'):
                fname = fname[0:len(fname)-1]

            # Add the directory part, and all prefixes of the directory
            # part, to the set of directories.
            dirpart = _get_dirpart(fname)
            while dirpart != '':
                #print("Added dir {}".format(dirpart))
                self._dirs.add(dirpart)
                dirpart = _get_dirpart(dirpart)

    def open(self, filename):
        return self._zipfile.open(filename)

    def isdir(self, path):
        return path in self._dirs

    def isfile(self, path):
        info = self._zipfile.getinfo(path)
        return info.file_size > 0 if info else False

    def listdir(self, path):
        #print("Getting contents of {}".format(path))
        result = set()
        pfx = "{}/".format(path)
        for info in self._zipfile.infolist():
            fname = info.filename
            if fname.startswith(pfx):
                # This entry is in the directory.
                # Get just the first directory component following
                # the directory part, and add it to the result.
                rest = fname[len(pfx):]
                if rest:
                    first_slash = rest.find('/')
                    member = rest[0:first_slash] if first_slash > 0 else rest
                    #print("Adding {}".format(member))
                    result.add(member)
        return sorted(list(result))

# Tag handler class: maps tag names to handler callbacks.
# Used for processing the lines of a progsnap data file
# (see the _scan function above).
# Unknown tags may be handled by registering a handler
# callback for the tag 'unknown': the default behavior for
# handling unknown tags is to ignore them.
class TagHandler():
    def __init__(self, callbacks={}):
        self._handlers = {}

        self._handlers['unknown'] = lambda tag, value: None

        for tagname in callbacks:
            self._handlers[tagname] = callbacks[tagname]

    # Register callback for specified tag name.
    def register_callback(self, tagname, tv_callback):
        self._handlers[tagname] = tv_callback

    # Register the same callback for multiple tag names.
    def register_callbacks(self, tagnames, tv_callback):
        for tagname in tagnames:
            self.register_callback(tagname, tv_callback)

    # Get the callback registered to handle specified tagname.
    # If there is no callback registered to handle the tagname,
    # the handler for the 'unknown' tag is returned.
    def get_callback(self, tagname):
        if tagname in self._handlers:
            return self._handlers[tagname]
        else:
            return self._handlers['unknown']

# This class makes a dictionary, most likely loaded from a JSON
# object, look like a Python object.  Each key in the dictionary
# can be treated as a method which, when called, returns the
# value associated with the key.  Pretty much all progsnap
# datatypes can be implemented effectively using this strategy.
class _HasProps():
    def __init__(self, *args):
        self._props = {}
        if args:
            self.add_all(args[0])

    def __getattr__(self, name):
        def getprop(*args):
            if name not in self._props:
                raise ProgsnapError("No such property: {}".format(name))
            return self._props[name]
        return getprop

    def setprop(self, tag, value):
        self._props[tag] = value

    def _setdefault(self, tag, defvalue):
        return self._props.setdefault(tag, defvalue)

    def add_all(self, other):
        self._props.update(other)

    def has(self, propname):
        return propname in self._props

class Test(_HasProps):
    def __init__(self, jsobj):
        super(Test, self).__init__(jsobj)

    # Opaque is an optional field, defaulting to false
    def opaque(self):
        return self._setdefault('opaque', False)

    # Invisible is an optional field, defaulting to false
    def invisible(self):
        return self._setdefault('invisible', False)

# This class represents both an Assignment value read from
# the assignments file, and also all metadata found by reading
# the corresponding assignment file.
class Assignment(_HasProps):
    def __init__(self, jsobj, access):
        super(Assignment, self).__init__(jsobj)
        self._access = access

        # Create tests list
        self.setprop('tests', [])

        # Read all additional info from the corresponding assignment file
        self._read_assign_file()

    def _read_assign_file(self):
        set_assign_prop = lambda tag, value: self.setprop(tag, value)
        add_test = lambda tag, value: self.tests().append(Test(value))
        th = TagHandler()
        th.register_callbacks(['name', 'language', 'url', 'assigned', 'due'], set_assign_prop)
        th.register_callback('test', add_test)

        with self._access.open(self.path()) as f:
            _scan(f, th)

        # Sort tests by test number
        self.tests().sort(key=lambda t: t.number())

class Student(_HasProps):
    def __init__(self, jsobj):
        super(Student, self).__init__(jsobj)

class Position(_HasProps):
    def __init__(self, jsobj):
        super(Position, self).__init__(jsobj)

def _fix_ts(ts):
    if not isinstance(ts, Number):
        # Quick fix for timestamp appearing as string rather than millis since epoch
        d = datetime.datetime.strptime(ts + '00', "%Y-%m-%d %H:%M:%S.%f%z")
        return int(d.timestamp() * 1000)
    return ts

class Edit(_HasProps):
    def __init__(self, jsobj):
        super(Edit, self).__init__(jsobj)
        self.setprop("ts", _fix_ts(self.ts())) # FIXME: temporary workaround

        # Promote start property to Position object
        if self.has("start"):
            self.setprop("start", Position(self.start()))

class Submission(_HasProps):
    def __init__(self, jsobj):
        super(Submission, self).__init__(jsobj)
        self.setprop("ts", _fix_ts(self.ts())) # FIXME: temporary workaround

class Compilation(_HasProps):
    def __init__(self, jsobj):
        super(Compilation, self).__init__(jsobj)
        self.setprop("ts", _fix_ts(self.ts())) # FIXME: temporary workaround

class TestResults(_HasProps):
    def __init__(self, jsobj):
        super(TestResults, self).__init__(jsobj)
        self.setprop("ts", _fix_ts(self.ts())) # FIXME: temporary workaround

# Work history object: events are loaded on demand
class WorkHistory():
    def __init__(self, access, assign_num, student_num, filename, sortworkhistory):
        self._events = []
        self._access = access
        self._assign_num = assign_num
        self._student_num = student_num
        self._filename = filename
        self._loaded = False
        self._sortworkhistory = sortworkhistory

    def assign_num(self):
        return self._assign_num

    def student_num(self):
        return self._student_num

    def _load_events(self):
        if not self._loaded:
            self._read_work_history()
            self._loaded = True

    def events(self):
        self._load_events()
        return self._events

    # Find all edit events with specified snapid.
    # Returns a list.
    def find_edit_events_with_snapid(self, snapid):
        self._load_events()
        result = []
        for evt in self._events:
            if type(evt) is Edit and evt.has("snapids") and snapid in evt.snapids():
                result.append(evt)
        return result

    # Find a single edit event with specified snapid.
    # Raises an error if multiple edit events are found (i.e.,
    # because the assignment has multiple source files).
    # Returns None if there is no edit event with the specified
    # snapid.
    def find_single_edit_event_with_snapid(self, snapid):
        candidates = self.find_edit_events_with_snapid(snapid)
        if len(candidates) == 0:
            return None
        if len(candidates) == 1:
            return candidates[0]
        raise ProgsnapError("There are multiple edit events for snapshot {}".format(snapid))

    def _find_event_with_snapid(self, evt_type, snapid):
        self._load_events()
        for evt in self._events:
            if (type(evt) is evt_type) and evt.snapid() == snapid:
                return evt
        return None

    # Find the Submission event with the specified snapid
    def find_submission_event(self, snapid):
        return self._find_event_with_snapid(Submission, snapid)

    # Find the Compilation event with the specified snapid
    def find_compilation_event(self, snapid):
        return self._find_event_with_snapid(Compilation, snapid)

    # Find the TestResults event with the specified snapid
    def find_testresults_event(self, snapid):
        return self._find_event_with_snapid(TestResults, snapid)

    def _read_work_history(self):
        th = TagHandler()
        th.register_callback('edit', lambda tag, value: self._events.append(Edit(value)))
        th.register_callback('submission', lambda tag, value: self._events.append(Submission(value)))
        th.register_callback('compilation', lambda tag, value: self._events.append(Compilation(value)))
        th.register_callback('testresults', lambda tag, value: self._events.append(TestResults(value)))

        with self._access.open(self._filename) as f:
            _scan(f, th)

        # Sort events if requested
        if self._sortworkhistory:
            def compare_events(a, b):
                # Compare by editids if possible
                if a.has("editid") and b.has("editid"):
                    cmp = a.editid() - b.editid()
                    if cmp != 0:
                        return cmp

                # Default to comparing by timestamps
                return a.ts() - b.ts()

            self._events.sort(key=functools.cmp_to_key(compare_events))

# The Dataset class is the top-level object allowing access
# to all of the information in a progsnap dataset.
class Dataset(_HasProps):
    def __init__(self, path, sortworkhistory=False):
        super(Dataset, self).__init__()

        if os.path.isdir(path):
            self._access = _ProgsnapDirectory(path)
        elif os.path.isfile(path):
            self._access = _ProgsnapZipfile(path)
        else:
            raise ProgsnapError("Path {} doesn't exist".format(path))

        self._assignments = []
        self._assignments_by_number = {}
        self._students = []
        self._students_by_number = {}
        # Map assignment numbers to list of work histories for the assignment
        self._assignment_work_histories = {}
        # Map student ids to list of work histories for the student
        self._student_work_histories = {}

        self._sortworkhistory = sortworkhistory

        self._read()

    def _add_student(self, jsobj):
        student = Student(jsobj)
        self._students.append(student)
        self._students_by_number[student.number()] = student

    def _add_assignment(self, jsobj):
        # Note that creating the Assignment object will also
        # read the metadata from the corresponding assignment file.
        assignment = Assignment(jsobj, self._access)
        self._assignments.append(assignment)
        self._assignments_by_number[assignment.number()] = assignment

    def _read(self):
        # Dataset file tag handler
        ds_th = TagHandler()
        ds_th.register_callbacks(
            ['psversion', 'name', 'contact', 'email', 'courseurl'],
            lambda tag, value: self.setprop(tag, value))

        with self._access.open("dataset.txt") as f:
            _scan(f, ds_th)

        # Assignments file tag handler
        assign_th = TagHandler(callbacks={'assignment':lambda tag, value: self._add_assignment(value)})

        with self._access.open("assignments.txt") as f:
            _scan(f, assign_th)

        # Students file is optional: read it if present
        if self._access.isfile("students.txt"):
            # Students file tag handler
            students_th = TagHandler(callbacks={'student':lambda key, value: self._add_student(value)})

            with self._access.open("students.txt") as f:
                _scan(f, students_th)

        # Find all of the work history files and organize them
        # by assignment and by student
        if self._access.isdir("history"):
            #print("History list: {}".format(self._access.listdir("history")))
            dirs = [e for e in self._access.listdir("history") if (_isint(e) and self._access.isdir("history/{}".format(e)))]
            dirs.sort(key=lambda x: int(x))
            for d in dirs:
                assign_num = int(d)
                hpath = "history/{}".format(d)
                #print("History dir: {0}".format(hpath))
                # Find list of work history files in the directory
                hist_files = [e for e in self._access.listdir(hpath) if (_ishistfile(e) and self._access.isfile("{}/{}".format(hpath, e)))]
                # Sort by student number
                hist_files.sort(key=lambda x: _extractint(x))
                for hf in hist_files:
                    #print("History file: {0}".format(hf))
                    hfpath = "{}/{}".format(hpath, hf)
                    student_num = _extractint(hf)

                    # If student isn't known, add a fake student entry,
                    # and arbitrarily assume that the student isn't an instructor.
                    if student_num not in self._students_by_number:
                        self._add_student({'number':student_num, 'instructor':False})

                    wh = WorkHistory(self._access, assign_num, student_num, hfpath, self._sortworkhistory)
                    self._assignment_work_histories.setdefault(assign_num, []).append(wh)
                    self._student_work_histories.setdefault(student_num, []).append(wh)

    def assignments(self):
        return self._assignments

    def assignment_for_number(self, assign_num):
        return self._assignments_by_number[assign_num]

    def students(self):
        return self._students

    def student_for_number(self, student_num):
        return self._students_by_number[student_num]

    def work_histories_for_assignment(self, assignment):
        return self._assignment_work_histories.setdefault(int(assignment.number()), [])

    def work_histories_for_student(self, student):
        return self._student_work_histories.setdefault(int(student.number()), [])

    def work_history_for_student_and_assignment(self, student, assignment):
        student_work_histories = self.work_histories_for_student(student)
        for wh in student_work_histories:
            if wh.assign_num() == assignment.number():
                return wh
        raise ProgsnapError("No work history for student {}, assignment {}".format(
            student.number(), assignment.number()))

def _line_chunks(text):
    pos = 0
    while pos < len(text):
        nextnl = text.find("\n", pos)
        if nextnl < 0:
            # Rest of string is last line,
            # and the text is not terminated by a newline.
            yield text[pos:]
            pos = len(text)
        else:
            # Found one more line (with terminating newline)
            yield text[pos:nextnl+1]
            # Advance to just past the newline
            pos = nextnl + 1

# Text document.
# The EditApplicator class can apply Edits to a TextDocument.
# This is useful for replaying a sequence of edits.
class TextDocument(object):
    VERIFY_DELETES = True

    def __init__(self):
        self._text = ""

    def get_num_lines(self):
        num_nl = self._text.count("\n")
        if not self._text.endswith("\n"):
            # There was text after the final newline
            # so that counts as an additional line.
            num_nl += 1
        return num_nl

    def get_line(self, index):
        count = 0
        for line in _line_chunks(self._text):
            if count == index:
                return line
            count += 1
        raise ProgsnapError("No such line in TextDocument: {}".format(index))

    def get_text(self):
        return self._text

    def set_text(self, text):
        self._text = text

    def __str__(self):
        return self._text

    def insert_at(self, row, col, text):
        pos = self._get_pos(row, col)
        updated_text = self._text[:pos] + text + self._text[pos:]
        self._text = updated_text

    def delete_at(self, row, col, text):
        pos = self._get_pos(row, col)
        endpos = pos + len(text)
        if endpos > len(self._text):
            # As an alarmingly special special case, if the deleted
            # text is attempting to remove a nonexistent \n from the
            # end of the document, let the delete proceed.
            # The ACE editor seems to add a newline to the end of
            # a file for reasons we're not entirely sure of.
            if endpos == len(self._text) + 1 and text.endswith("\n"):
                endpos -= 1
                text = text[:len(text)-1]
            else:
                raise ProgsnapError("Deletion beyond end of TextDocument (pos={}, len={})".format(endpos, len(self._text)))
        #print("Deleting from {} to {}".format(pos, endpos))
        if TextDocument.VERIFY_DELETES:
            to_remove = self._text[pos:endpos]
            if to_remove != text:
                raise ProgsnapError("Delete mismatch: expected {}, saw {}".format(json.dumps(text), json.dumps(to_remove)))
        updated_text = self._text[:pos] + self._text[endpos:]
        self._text = updated_text

    # Get the position (string index) of given row/col pair.
    def _get_pos(self, row, col):
        atrow = 0
        pos = 0

        # Skip to the beginning of the desired row
        while atrow < row:
            nextpos = self._text.find("\n", pos)
            if nextpos < 0:
                raise ProgsnapError("No such line in TextDocument: {}".format(row))
            # Advance to beginning of next line
            pos = nextpos + 1
            atrow += 1

        pos += col
        if pos  > len(self._text):
            raise ProgsnapError("Position {}:{} in TextDocument is out of bounds (pos={}, len={})".format(row, col, pos, len(self._text)))
        return pos

class EditApplicator:
    def __init__(self):
        pass

    def apply(self, edit, text_document):
        if edit.type() == 'insert':
            self._do_insert(edit, text_document)
        elif edit.type() == 'delete':
            self._do_delete(edit, text_document)
        elif edit.type() == 'fulltext':
            self._do_fulltext(edit, text_document)
        else:
            raise ProgsnapError("Don't know how to handle edit type {}".format(edit.type()))

    def _do_insert(self, edit, text_document):
        text_document.insert_at(edit.start().row(), edit.start().col(), edit.text())

    def _do_delete(self, edit, text_document):
        text_document.delete_at(edit.start().row(), edit.start().col(), edit.text())

    def _do_fulltext(self, edit, text_document):
        # Easy: replace text with new text
        text_document.set_text(edit.text())

# vim:set expandtab:
# vim:set tabstop=4:
