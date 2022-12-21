package nifitest

type errorChain struct {
	next *errorChain
	err  error
}

func chainErrors(e1, e2 error) error {
	if e1 != nil {
		if e2 != nil {
			ec, ok := e1.(*errorChain)
			if !ok {
				ec = &errorChain{
					next: nil,
					err: e1,
				}
			}
			return ec.chain(e2)
		}
		return e1
	}
	return e2
}

func (e *errorChain) chain(e2 error) error {
	if e.next != nil {
		e.next.chain(e2)
	} else {
		e.next = &errorChain{next: nil, err: e2}
	}
	return e
}

func (e errorChain) Error() string {
	s1 := e.err.Error()
	if e.next != nil {
		s2 := e.next.Error()
		if len(s1) > 0 {
			if len(s2) > 0 {
				return s1 + ". " + s2
			}
			return s1
		}
		return s2
	}
	return s1
}

func (e *errorChain) Unwrap() error {
	if e.next != nil {
		return e.next
	}
	return nil
}
