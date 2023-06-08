authenti-chatai-cloudbuild: chatbot for product verification
=====================================================================

This package can be used from within other python packages, or as a standalone CLI service.



# Usage
### Requirements
#### Python - v3.9 minimum

## Setup for usage
### For using from other python packages
To use exchange-rate-importer as an imported package from another python project:
1. install `authenti-chatai-cloudbuild` in the current project's virtual environment by importing the wheel distribution. E.g.:
   ```shell
   # if the wheel package is in the current folder
   $ pip install authenti_chatai_cloudbuild-0.1.3-py2.py3-none-any.whl
   
   # if the wheel package is in another location
   $ pip install <path to authenti_chatai_cloudbuild.whl>
   ```
### For using a standalone package
1. Set up a virtual environment (or conda environment). Although not mandatory, it is highly
   recommended separating each project's python environment. To create a virtual environment
   in the project directory itself with the native Python environment manager `venv`:
    ```bash
    $ cd /path/to/project/directory
    $ python3 -m venv .venv #sets up a new virtual env called .venv
    ```
   Next, to activate the virtual environment (say `.venv`):
    ```bash
    $ source .venv/Scripts/activate
    ```

# Testing

Tests are added in the `tests` dir.
1. To run all tests simply run:
   ```bash 
   $ pytest 
   ```
2. To run all the tests from one directory, use the directory as a parameter to pytest:
   ```bash
   $ pytest tests/my-directory
   ```
3. To run all tests in a file , list the file with the relative path as a parameter to pytest:
   ```bash
   $ pytest tests/my-directory/test_demo.py
   ```
4. To run a set of tests based on function names, the -k option can be used
   For example, to run all functions that have _raises in their name:
   ````shell
   $ pytest -v -k _raises
   ````