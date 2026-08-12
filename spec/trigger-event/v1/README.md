# Trigger event v1

`TriggerEvent` is normalized ingress evidence, not Action inputs. Its safe public payload is frozen before admission; raw bodies and attachments remain Artifact references. Headers, cookies, secret values, and ambient environment state are not payload fields.

The six public ingress kinds and the internal child-action kind all use the same admission contract. Their candidate origin is checked by the kernel.
