package utils

func SwapSlicInner(srcOffset [2]uint, dstOffset [2]uint, aimSlic []byte) []byte {
	t1 := aimSlic[srcOffset[0]:srcOffset[1]]
	t2 := aimSlic[dstOffset[0]:dstOffset[1]]

	res := append([]byte{}, aimSlic[:srcOffset[0]]...)
	res = append(res, t2...)
	res = append(res, aimSlic[srcOffset[1]:dstOffset[0]]...)
	res = append(res, t1...)
	res = append(res, aimSlic[dstOffset[1]:]...)
	return res
}
