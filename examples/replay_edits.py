"""
Usage:
  replay_edits.py [-v] [-f <outdir>] <datazip> [<student_num>]

Options:
  -v           Verbose mode (print full text following each edit)
  -f <outdir>  Save text full text after each edit to files in <outdir>
"""

# This is a test program for tracking of fine-grained edits.

import progsnap

from helpers import Report

# Compare actual and expected text.
# "actual" is the product of EditApplicator.
# "expected" is reference text, e.g., a fulltext edit.
def match_text(actual, expected):
    # For whatever reason, some fulltext edits seem to gain
    # a newline at the end of the document, even though there is
    # clearly no edit in which that newline is added.
    # So, if the actual text matches everything but the
    # final newline, that's ok.
    return actual == expected or actual + "\n" == expected or actual == expected + "\n"

if __name__ == '__main__':
    from docopt import docopt
    arguments = docopt(__doc__)
    datazip = arguments.get("<datazip>")
    student_num = arguments.get("<student_num>")
    verbose = arguments.get("-v")
    outdir = arguments.get("-f")

    dataset = progsnap.Dataset(datazip)

    if student_num:
        student_num = int(student_num)

    for s in dataset.students():
        if student_num and student_num != s.number():
            continue
        for wh in dataset.work_histories_for_student(s):
            print("Student {}, problem {}".format(wh.student_num(), wh.assign_num()))
            doc = progsnap.TextDocument()
            applicator = progsnap.EditApplicator()
            last_text = None
            last_editid = None
            first_fulltext = None
            for evt in wh.events():
                if type(evt) is progsnap.Edit:
                    # Save the first fulltext edit in the edit sequence:
                    # if the user resets his/her code, it will revert to
                    # this.  Resets should not be counted as synchronization
                    # errors.
                    if not first_fulltext and evt.type() == 'fulltext':
                        first_fulltext = evt.text()

                    if verbose:
                        print("{} {} {} -------------------------".format(wh.student_num(),evt.editid(),evt.type()))
                    applicator.apply(evt, doc)

                    if outdir:
                        with open("{}/{}.txt".format(outdir, evt.editid()), 'w') as out:
                            print(doc.get_text(), file=out, end='')

                    if verbose:
                        print(doc.get_text())
                        print("-------------------------")

                    if evt.type() == 'fulltext' and last_text:
                        matches = match_text(last_text, doc.get_text())
                        matches_first_fulltext = first_fulltext and match_text(first_fulltext, doc.get_text())
                        if not (matches or matches_first_fulltext):
                            raise progsnap.ProgsnapError("Fulltext not in sync at editid={}, last edit={}".format(evt.editid(), last_editid))

                    last_text = doc.get_text()
                    last_editid = evt.editid()

# vim:set expandtab:
# vim:set tabstop=4:
