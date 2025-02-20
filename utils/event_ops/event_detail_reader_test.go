package event_ops

import (
	"fmt"
	"testing"
)

func Test_Read_int_try(t *testing.T) {
	// \x80\x00\x00

	bdata := []byte{0x80, 0x00, 0x00}
	x := EventDetailReader{}
	n := x.Read_int_try(bdata, "big")
	fmt.Println(n)

	a := 1 << (24 - 1)
	fmt.Println(a)

	data := []byte{0xe0, 0xff, 0xff}
	// little uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	// big    uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
	result := uint32(data[2])<<16 | uint32(data[1])<<8 | uint32(data[0])
	a3r := x.Read_uint_try(data, "little")
	fmt.Printf("%b(%d) %b(%d)\n", result, result, a3r, a3r)
	//  111000001111111111111111(14745599) 11111111111111111110000000000000(4294959104)

	var a4x string = "test"
	func() {
		a4x = "xxxx"
	}()
	fmt.Printf("%v", a4x)
	fmt.Println("")

	nullbits := []byte{0x00, 0x00, 0xfe}
	x99 := x.Read_uint_try(nullbits, "")
	x99 = 0b100000000010100000000000
	for x := 0; x < 25; x++ {
		fmt.Printf("%b\n", x99)
		func() {
			tmx := fmt.Sprintf("%b\n", 1<<x)
			for len(tmx) <= len(fmt.Sprintf("%b", x99)) {
				tmx = "0" + tmx
			}
			fmt.Printf("%s\n", tmx)
		}()
		// fmt.Printf("%b\n", 1<<x)
		fmt.Println("x:", x, "res:", x99&(1<<x))

		fmt.Println("--------------------------------------")
	}
	// 111111100000000000000000
	// 10000000000000000

}

// 111111111111111111100000
// 111111111111111111100000
