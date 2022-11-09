pub fn compose_err<F, G, T, U, V, E>(f: F, g: G) -> impl Fn(T) -> Result<V, E>
where
    F: Fn(T) -> Result<U, E>,
    G: Fn(U) -> Result<V, E>,
{
    move |t: T| {
        let u: U = f(t)?;
        g(u)
    }
}

pub fn compose<F, G, T, U, V>(f: F, g: G) -> impl Fn(T) -> V
where
    F: Fn(T) -> U,
    G: Fn(U) -> V,
{
    move |t: T| {
        let u: U = f(t);
        g(u)
    }
}
