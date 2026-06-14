package client

func LocalGet(
	address string,
	key string,
) (string, error) {

	return ForwardCommand(
		address,
		"LOCAL_GET "+key,
	)
}