package storage

import (
 "os"
 "syscall"
)

// Opening with no sharing prevents two broker processes using the same directory.
// Windows releases the handle even if the broker crashes.
func Lock(path string) (*os.File,error) {
 p,err:=syscall.UTF16PtrFromString(path); if err!=nil { return nil,err }
 h,err:=syscall.CreateFile(p,syscall.GENERIC_READ|syscall.GENERIC_WRITE,0,nil,syscall.OPEN_ALWAYS,syscall.FILE_ATTRIBUTE_NORMAL,0)
 if err!=nil { return nil,err }
 return os.NewFile(uintptr(h),path),nil
}
