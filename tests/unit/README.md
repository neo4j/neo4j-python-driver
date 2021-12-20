Structure:

```
.
├── _async_compat  # utility code to allow auto-converting async tests to sync tests
├── async          # tests that are specifit to async classes, methods, or functions  
├── common         # tests of classes, methods, and functions that only exist in sync
├── mixed          # tests that cannot be auto converted from async to sync
└── sync           # auto-genereated sync versions of tetst in async
```
