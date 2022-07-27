use crate::accounts_data::*;
use crate::network_protocol::testonly as data;
use crate::network_protocol::SignedAccountData;
use crate::testonly::{assert_is_superset, make_rng, AsSet as _, Rng};
use near_network_primitives::time;
use near_network_primitives::types::AccountKeys;
use near_primitives::types::EpochId;
use pretty_assertions::assert_eq;
use std::sync::Arc;

// run a trivial future until completion => OK
#[tokio::test]
async fn must_complete_ok() {
    assert_eq!(5, must_complete(async move { 5 }).await);
}

// drop a trivial future without completion => panic (in debug mode at least).
#[tokio::test]
#[should_panic]
async fn must_complete_should_panic() {
    let _ = must_complete(async move { 6 });
}

struct Signer {
    epoch_id: EpochId,
    signer: near_crypto::InMemorySigner,
}

impl Signer {
    fn make_account_data(&self, rng: &mut Rng, timestamp: time::Utc) -> SignedAccountData {
        data::make_account_data(
            rng,
            timestamp,
            self.epoch_id.clone(),
            self.signer.account_id.clone(),
        )
        .sign(&self.signer)
        .unwrap()
    }
}

fn unwrap<'a, T: std::hash::Hash + std::cmp::Eq, E: std::fmt::Debug>(
    v: &'a (T, Option<E>),
) -> &'a T {
    if let Some(err) = &v.1 {
        panic!("unexpected error: {err:?}");
    }
    &v.0
}

fn make_signers(rng: &mut Rng, n: usize) -> Vec<Signer> {
    (0..n)
        .map(|_| Signer { epoch_id: data::make_epoch_id(rng), signer: data::make_signer(rng) })
        .collect()
}

fn make_account_keys(signers: &[Signer]) -> Arc<AccountKeys> {
    Arc::new(
        signers
            .iter()
            .map(|s| {
                ((s.epoch_id.clone(), s.signer.account_id.clone()), s.signer.public_key.clone())
            })
            .collect(),
    )
}

#[tokio::test]
async fn happy_path() {
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();
    let now = clock.now_utc();

    let signers: Vec<_> = make_signers(rng, 7);
    let e0 = make_account_keys(&signers[0..5]);
    let e1 = make_account_keys(&signers[2..7]);

    let cache = Arc::new(Cache::new());
    assert_eq!(cache.dump(), vec![]); // initially empty
    assert!(cache.set_keys(e0.clone()));
    assert_eq!(cache.dump(), vec![]); // empty after initial set_keys.

    // initial insert
    let a0 = signers[0].make_account_data(rng, now);
    let a1 = signers[1].make_account_data(rng, now);
    let res = cache.clone().insert(vec![a0.clone(), a1.clone()]).await;
    assert_eq!([&a0, &a1].as_set(), unwrap(&res).as_set());
    assert_eq!([&a0, &a1].as_set(), cache.dump().as_set());

    // entries of various types
    let a0new = signers[0].make_account_data(rng, now + time::Duration::seconds(1));
    let a1old = signers[1].make_account_data(rng, now - time::Duration::seconds(1));
    let a2 = signers[2].make_account_data(rng, now);
    let a5 = signers[5].make_account_data(rng, now);
    let res = cache
        .clone()
        .insert(vec![
            a2.clone(),    // initial value => insert
            a0new.clone(), // with newer timestamp => insert,
            a1old.clone(), // with older timestamp => filter out,
            a5.clone(),    // not in e0 => filter out.
        ])
        .await;
    assert_eq!([&a2, &a0new].as_set(), unwrap(&res).as_set());
    assert_eq!([&a0new, &a1, &a2].as_set(), cache.dump().as_set());

    // try setting the same key set again, should be a noop.
    assert!(!cache.set_keys(e0));
    assert_eq!([&a0new, &a1, &a2].as_set(), cache.dump().as_set());

    // set_keys again. Data for accounts which are not in the new set should be dropped.
    assert!(cache.set_keys(e1));
    assert_eq!([&a2].as_set(), cache.dump().as_set());
    // insert some entries again.
    let res = cache
        .clone()
        .insert(vec![
            a0.clone(), // a0 is not in e1 => filter out
            a5.clone(), // a5 is in e1 => insert,
        ])
        .await;
    assert_eq!([&a5].as_set(), unwrap(&res).as_set());
    assert_eq!([&a2, &a5].as_set(), cache.dump().as_set());
}

#[tokio::test]
async fn data_too_large() {
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();
    let now = clock.now_utc();

    let signers = make_signers(rng, 3);
    let e = make_account_keys(&signers);

    let cache = Arc::new(Cache::new());
    cache.set_keys(e);
    let a0 = signers[0].make_account_data(rng, now);
    let a1 = signers[1].make_account_data(rng, now);
    let mut a2_too_large = signers[2].make_account_data(rng, now);
    *a2_too_large.payload_mut() =
        (0..crate::network_protocol::MAX_ACCOUNT_DATA_SIZE_BYTES + 1).map(|_| 17).collect();

    // too large payload => DataTooLarge
    let res = cache
        .clone()
        .insert(vec![
            a0.clone(),
            a1.clone(),
            a2_too_large.clone(), // invalid entry => DataTooLarge
        ])
        .await;
    assert_eq!(Some(Error::DataTooLarge), res.1);
    // Partial update is allowed, in case an error is encountered.
    assert_is_superset(&[&a0, &a1].as_set(), &res.0.as_set());
    // Partial update should match the state.
    assert_eq!(res.0.as_set(), cache.dump().as_set());
}

#[tokio::test]
async fn invalid_signature() {
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();
    let now = clock.now_utc();

    let signers = make_signers(rng, 3);
    let e = make_account_keys(&signers);

    let cache = Arc::new(Cache::new());
    cache.set_keys(e);
    let a0 = signers[0].make_account_data(rng, now);
    let mut a1 = signers[1].make_account_data(rng, now);
    let mut a2_invalid_sig = signers[2].make_account_data(rng, now);
    *a2_invalid_sig.signature_mut() = a1.signature_mut().clone();

    // invalid signature => InvalidSignature
    let res = cache
        .clone()
        .insert(vec![
            a0.clone(),
            a1.clone(),
            a2_invalid_sig.clone(), // invalid entry => DataTooLarge
        ])
        .await;
    assert_eq!(Some(Error::InvalidSignature), res.1);
    // Partial update is allowed, in case an error is encountered.
    assert_is_superset(&[&a0, &a1].as_set(), &res.0.as_set());
    // Partial update should match the state.
    assert_eq!(res.0.as_set(), cache.dump().as_set());
}

#[tokio::test]
async fn single_account_multiple_data() {
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();
    let now = clock.now_utc();

    let signers = make_signers(rng, 3);
    let e = make_account_keys(&signers);

    let cache = Arc::new(Cache::new());
    cache.set_keys(e);
    let a0 = signers[0].make_account_data(rng, now);
    let a1 = signers[1].make_account_data(rng, now);
    let a2old = signers[2].make_account_data(rng, now);
    let a2new = signers[2].make_account_data(rng, now + time::Duration::seconds(1));

    // 2 entries for the same (epoch_id,account_id) => SingleAccountMultipleData
    let res =
        cache.clone().insert(vec![a0.clone(), a1.clone(), a2old.clone(), a2new.clone()]).await;
    assert_eq!(Some(Error::SingleAccountMultipleData), res.1);
    // Partial update is allowed, in case an error is encountered.
    assert_is_superset(&[&a0, &a1, &a2old, &a2new].as_set(), &res.0.as_set());
    // Partial update should match the state, this also verifies that only 1 of the competing
    // entries has been applied.
    assert_eq!(res.0.as_set(), cache.dump().as_set());
}