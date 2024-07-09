# Contributing to the Neo4j Ecosystem

At [Neo4j](https://neo4j.com/), we develop our software in the open at GitHub.
This provides transparency for you, our users, and allows you to fork the software to make your own additions and enhancements.
We also provide areas specifically for community contributions, in particular the [neo4j-contrib](https://github.com/neo4j-contrib) space.

There's an active [Neo4j Online Community](https://community.neo4j.com/) where we work directly with the community.
If you're not already a member, sign up!

We love our community and wouldn't be where we are without you.


## Need to raise an issue?

Where you raise an issue depends largely on the nature of the problem.

Firstly, if you are an Enterprise customer, you might want to head over to our [Customer Support Portal](https://support.neo4j.com/).

There are plenty of public channels available too, though.
If you simply want to get started or have a question on how to use a particular feature, ask a question in [Neo4j Online Community](https://community.neo4j.com/).
If you think you might have hit a bug in our software (it happens occasionally!) or you have specific feature request then use the issue feature on the relevant GitHub repository.
Check first though as someone else may have already raised something similar.

[StackOverflow](https://stackoverflow.com/questions/tagged/neo4j) also hosts a ton of questions and might already have a discussion around your problem.
Make sure you have a look there too.

Include as much information as you can in any request you make:

- Which versions of our products are you using?
- Which language (and which version of that language) are you developing with?
- What operating system are you on?
- Are you working with a cluster or on a single machine?
- What code are you running?
- What errors are you seeing?
- What solutions have you tried already?


## Want to contribute?

If you want to contribute a pull request, we have a little bit of process you'll need to follow:

- Do all your work in a personal fork of the original repository
- [Rebase](https://github.com/edx/edx-platform/wiki/How-to-Rebase-a-Pull-Request), don't merge (we prefer to keep our history clean)
- Create a branch (with a useful name) for your contribution
- Make sure you're familiar with the appropriate coding style (this varies by language so ask if you're in doubt)
- Include unit tests if appropriate (obviously not necessary for documentation changes)
- Take a moment to read and sign our [CLA](https://neo4j.com/developer/cla)

We can't guarantee that we'll accept pull requests and may ask you to make some changes before they go in.
Occasionally, we might also have logistical, commercial, or legal reasons why we can't accept your work but we'll try to find an alternative way for you to contribute in that case.
Remember that many community members have become regular contributors and some are now even Neo employees!


## Specifically for this project:

All code in `_sync` or `sync` folders is auto-generated. Don't change it, but
install the pre-commit hooks as described below instead. They will take care of
updating the code if necessary.

### Setting up the Development Environment
Setting up the development environment:
 * Install Python 3.8+
 * Install the requirements
   ```bash
   $ python3 -m pip install -U pip
   $ python3 -m pip install -Ur requirements-dev.txt
   ```
 * Install the pre-commit hook, that will do some code-format-checking everytime
   you commit.
   ```bash
   $ pre-commit install
   ```


### Working with Pre-commit
If you want to run the pre-commit checks manually, you can do so:
```bash
$ pre-commit run --all-files
# or
$ pre-commit run --file path/to/a/file
```

To commit skipping the pre-commit checks, you can do so:
```bash
git commit --no-verify ...
```

### Running Tests
```bash
# in the project root

# for unit tests
python -m tox -f unit  # more thorough but complex way; requires python 3.7 - 3.12 to be installed
python -m pytest -W error tests/unit  # only test with current python version; no code coverage

# integration tests
# requires
#  * running DBMS
#  * env variables to be set for the tests to learn about the DBMS they run against
python -m tox -f integration  # option 1
python -m pytest -W error tests/integration  # option 2

# performance tests (see requirements for integration tests)
python -m tox -f performance  # option 1
python -m pytest -W error --benchmark-autosave --benchmark-group-by=fullname tests/performance  # option 2
# add --benchmark-compare to option 2 to compare with the previous run
```


## Got an idea for a new project?

If you have an idea for a new tool or library, start by talking to other people in the community.
Chances are that someone has a similar idea or may have already started working on it.
The best software comes from getting like minds together to solve a problem.
And we'll do our best to help you promote and co-ordinate your Neo4j ecosystem projects.


## Further reading

If you want to find out more about how you can contribute, head over to our website for [more information](https://neo4j.com/developer/contributing-code/).
