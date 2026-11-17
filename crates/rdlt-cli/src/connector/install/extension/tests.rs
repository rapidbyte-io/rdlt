use super::extension;

#[test]
fn an_extension_is_read_as_pip_s_splitext_reads_it() {
    for (path, read) in [
        ("tickets-0.1.0-py3-none-any.whl", ".whl"),
        ("tickets-0.1.0-py3-none-any.tar.whl", ".tar.whl"),
        ("tickets-0.1.0-py3-none-x.TAR.whl", ".TAR.whl"),
        ("tickets-0.1.0-py3.tar-none-any.whl", ".whl"),
        ("a.tar.gz", ".tar.gz"),
        ("a.tar", ".tar"),
        ("a.b/.tar", ".tar"),
        (".tar", ".tar"),
        ("a..whl", ".whl"),
        (".whl", ""),
        ("..whl", ""),
        ("a/.whl", ""),
        ("a.b/c", ""),
        ("zo\u{eb}.Tar.whl", ".Tar.whl"),
        ("\u{eb}.whl", ".whl"),
        ("", ""),
    ] {
        assert_eq!(extension(path), read, "{path:?}");
    }
}
