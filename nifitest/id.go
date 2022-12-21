package nifitest

type idImpl struct {
	group, id string
}

func (i *idImpl) Id() string {
	return i.id
}

func (i *idImpl) GroupId() string {
	return i.group
}

func NewId(grp, id string) Id {
	return &idImpl{group: grp, id: id}
}
