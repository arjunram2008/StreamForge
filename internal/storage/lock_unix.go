//go:build linux || darwin

package storage

import (
 "os"
 "syscall"
)

func Lock(path string) (*os.File,error) {
 f,err:=os.OpenFile(path,os.O_CREATE|os.O_RDWR,0644); if err!=nil { return nil,err }
 if err:=syscall.Flock(int(f.Fd()),syscall.LOCK_EX|syscall.LOCK_NB); err!=nil { f.Close(); return nil,err }
 return f,nil
}
