use rdlt_host::Lingering;

use super::{both, ending, interrupted, signalled};
use crate::cli::{Ended, FINDINGS, IO, USAGE};

fn lingering(groups: &[u32]) -> Result<(), Lingering> {
    Err(Lingering {
        groups: groups.to_vec(),
    })
}

fn ended<T>(ending: Result<T, Ended>) -> (u8, String) {
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
    assert_eq!(code, IO);
    assert!(message.contains("[41]"), "{message}");
    // One that failed keeps its failure, and says what it left beside it.
    let failed = || Err::<u8, _>(Ended(USAGE, "no such option".to_owned()));
    assert_eq!(
        ended(ending(failed(), Ok(()), None)),
        (USAGE, "no such option".to_owned())
    );
    let (code, message) = ended(ending(failed(), lingering(&[41, 42]), None));
    assert_eq!(code, USAGE);
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
    assert_eq!(code, 130);
    assert!(
        message.starts_with("interrupted") && message.contains("[9]"),
        "{message}"
    );
}

#[test]
fn the_last_signal_heard_says_how_the_certification_exits() {
    // Heard only as its connectors stopped, a signal still ends it as a signal does.
    assert_eq!(ended(ending(Ok(7), Ok(()), Some(143))).0, 143);
    // Heard twice, the last says the exit.
    assert_eq!(
        ended(ending(Err::<u8, _>(interrupted(130)), Ok(()), Some(131))).0,
        131
    );
    // A failure of its own is not taken for a signal's, whatever is heard after.
    let failed = Err::<u8, _>(Ended(FINDINGS, "nothing was certified".to_owned()));
    assert_eq!(ended(ending(failed, Ok(()), Some(129))).0, FINDINGS);
    for status in [129, 130, 131, 143] {
        assert_eq!(signalled(&interrupted(status)), Some(status));
    }
    for code in [0, FINDINGS, 2, USAGE, IO, 128] {
        assert_eq!(signalled(&Ended(code, String::new())), None, "{code}");
    }
}

#[test]
fn two_stops_of_the_same_groups_name_each_lingering_group_once() {
    assert_eq!(both(Ok(()), Ok(())), Ok(()));
    assert_eq!(both(lingering(&[3]), Ok(())), lingering(&[3]));
    assert_eq!(both(Ok(()), lingering(&[3])), lingering(&[3]));
    assert_eq!(
        both(lingering(&[5, 3]), lingering(&[3, 4])),
        lingering(&[3, 4, 5])
    );
}
