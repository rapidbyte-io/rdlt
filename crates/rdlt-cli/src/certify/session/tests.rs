use rdlt_host::Lingering;

use super::{ending, interrupted, signalled};
use crate::Exit;
use crate::certify::Ended;

fn lingering(groups: &[u32]) -> Result<(), Lingering> {
    Err(Lingering {
        groups: groups.to_vec(),
    })
}

fn ended<T>(ending: Result<T, Ended>) -> (Exit, String) {
    let Err(Ended(code, message)) = ending else {
        panic!("the certification did not end in error");
    };
    (code, message)
}

#[test]
fn a_group_that_lingers_is_said_however_the_certification_ended() {
    assert_eq!(ending(Ok(7), Ok(()), None).ok(), Some(7));
    // A certification that ran to its end fails for what it left.
    let (code, message) = ended(ending(Ok(7), lingering(&[41]), None));
    assert_eq!(code, Exit::Io);
    assert!(message.contains("[41]"), "{message}");
    // One that failed keeps its failure, and says what it left beside it.
    let failed = || Err::<u8, _>(Ended(Exit::Usage, "no such option".to_owned()));
    assert_eq!(
        ended(ending(failed(), Ok(()), None)),
        (Exit::Usage, "no such option".to_owned())
    );
    let (code, message) = ended(ending(failed(), lingering(&[41, 42]), None));
    assert_eq!(code, Exit::Usage);
    assert!(
        message.starts_with("no such option; ") && message.contains("[41, 42]"),
        "{message}"
    );
    // One that was interrupted exits as the signal has it, and says what it left too.
    let (code, message) = ended(ending(
        Err::<u8, _>(interrupted(130)),
        lingering(&[9]),
        Some(130),
    ));
    assert_eq!(code, Exit::Signalled(130));
    assert!(
        message.starts_with("interrupted") && message.contains("[9]"),
        "{message}"
    );
}

#[test]
fn the_last_signal_heard_says_how_the_certification_exits() {
    // Heard only as its connectors stopped, a signal still ends it as a signal does.
    assert_eq!(
        ended(ending(Ok(7), Ok(()), Some(143))).0,
        Exit::Signalled(143)
    );
    // Heard twice, the last says the exit.
    assert_eq!(
        ended(ending(Err::<u8, _>(interrupted(130)), Ok(()), Some(131))).0,
        Exit::Signalled(131)
    );
    // A failure of its own is not taken for a signal's, whatever is heard after.
    let failed = Err::<u8, _>(Ended(Exit::Findings, "nothing was certified".to_owned()));
    assert_eq!(ended(ending(failed, Ok(()), Some(129))).0, Exit::Findings);
    for status in [129, 130, 131, 143] {
        assert_eq!(signalled(&interrupted(status)), Some(status));
    }
    for exit in [
        Exit::Success,
        Exit::Findings,
        Exit::Config,
        Exit::Usage,
        Exit::Io,
    ] {
        assert_eq!(signalled(&Ended(exit, String::new())), None, "{exit:?}");
    }
}
