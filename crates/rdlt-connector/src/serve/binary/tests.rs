use super::{Args, Failure, parse};

fn parsed(args: &[&str]) -> Result<Args, Failure> {
    parse(args.iter().map(|arg| (*arg).to_owned()))
}

#[test]
fn the_hosts_socket_is_named_either_way() {
    assert_eq!(parsed(&["--rdlt-fd", "3"]), Ok(Args { fd: 3 }));
    assert_eq!(parsed(&["--rdlt-fd=7"]), Ok(Args { fd: 7 }));
}

#[test]
fn anything_else_is_refused_with_why() {
    let cases: [(&[&str], &str); 6] = [
        (&[], "started by its host"),
        (&["--rdlt-fd"], "needs a file descriptor"),
        (&["--rdlt-fd", "three"], "is not a file descriptor"),
        (&["--rdlt-fd=3", "--rdlt-fd=4"], "given twice"),
        (&["--rdlt-fdx"], "needs a file descriptor"),
        (&["--listen", "0.0.0.0:1"], "unknown argument `--listen`"),
    ];
    for (args, why) in cases {
        let refused = parsed(args).unwrap_err().to_string();
        assert!(refused.contains(why), "{args:?}: {refused}");
    }
}
