ignore_newline = r'aaa\n+bbb'
def ignore_newline(self, t):
    self.lineno += t.value.count('\n')
    print(ignore_newline)
    