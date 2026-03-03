When reviewing code, focus on:

## Security Critical Issues
- Check for hardcoded secrets, API keys, or credentials
- Check for instances of potential method call injection, dynamic code execution, symbol injection or other code injection vulnerabilities.

## Performance Red Flags
- Spot inefficient loops and algorithmic issues.
- Check for memory leaks and resource cleanup.

## Code Quality Essentials
- Methods should be focused and appropriately sized. If a method is doing too much, suggest refactorings to split it up.
- Use clear, descriptive naming conventions.
- Avoid encapsulation violations and ensure proper separation of concerns.
- All public classes, modules, and methods should have clear documentation in Sphinx format.

## PyMongo-specific Concerns
- Do not review files within `pymongo/synchronous` or files in `test/` that also have a file of the same name in `test/asynchronous` unless the reviewed changes include a `_IS_SYNC` statement. PyMongo generates these files from `pymongo/asynchronous` and `test/asynchronous` using `tools/synchro.py`.
- All asynchronous functions must not call any blocking I/O.

## Review Style
- Be specific and actionable in feedback.
- Explain the "why" behind recommendations.
- Acknowledge good patterns when you see them.
- Ask clarifying questions when code intent is unclear.

Always prioritize security vulnerabilities and performance issues that could impact users.

Always suggest changes to improve readability and testability. For example, this suggestion seeks to make the code more readable, reusable, and testable:

```python
# Instead of:
if user.email and user.email.contains("@") and len(user.email) > 5:
    submit_button.enabled = True
else:
    submit_button.enabled = False

# Consider:
def valid_email(email):
    return email and email.contains("@") and len(email) > 5


submit_button.enabled = valid_email(user.email)
```
