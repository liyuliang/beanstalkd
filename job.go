package beanstalkd

type Job struct {
	ID   uint64
	Body []byte
}

