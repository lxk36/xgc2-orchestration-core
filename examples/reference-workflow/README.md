# Hello reference workflow

This standalone definition proves the v1 compiler without any product source tree. It binds an Action input into `prepare`, then passes one typed output over an explicit data edge into `render`.

S1 compiles and checks this definition; it does not yet dispatch workers or claim durable execution.
