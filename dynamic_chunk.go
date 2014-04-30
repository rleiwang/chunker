/*
 * Anchor driven variable length chunk, randomized cyclic polynomial rolling hash
 *
 */
package chunker

import (
	"crypto/sha1"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

const (
	min   = 1920
	max   = 2048
	winSZ = 32
	bufSZ = 4096
)

func reset(dir string) (*hash.Hash, *os.File, error) {
	h := sha1.New()
	f, err := ioutil.TempFile(dir, "working_")
	return &h, f, err
}

func writeSegment(seg []byte, f *os.File, h *hash.Hash) {
	f.Write(seg)
	(*h).Write(seg)
}

func closeChunk(f *os.File, h *hash.Hash) (name string, size int64, err error) {
	f.Sync()
	fi, _ := f.Stat()
	size = fi.Size()
	f.Close()
	name = fmt.Sprintf("%x", (*h).Sum(nil))
	fname := f.Name()
	err = os.Rename(fname, filepath.Join(filepath.Dir(fname), name))
	return
}

// Chunk file into pieces and put in dir
func Chunking(file string, dir string) ([]string, error) {
	var chunks []string
	buf := make([]byte, bufSZ)
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var rh rollingHash

	tail, head := 0, 0
	loaded, marker, offset := 0, 0, 0

	// prev is filled to last winSZ if the chunk is squating between two loaded buffers
	prev := make([]byte, winSZ)
	var name string

	hasher, chunker, err := reset(dir)
	if err != nil {
		return nil, err
	}

	for {
		loaded, err = f.Read(buf)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		}
		marker, offset = 0, 0
		for offset < loaded {
			if head == tail+max {
				// write out chunk and start over
				if offset > marker {
					writeSegment(buf[marker:offset], chunker, hasher)
				}
				name, _, err = closeChunk(chunker, hasher)
				if err != nil {
					return nil, err
				}
				chunks = append(chunks, name)

				marker = offset
				tail = head
				hasher, chunker, err = reset(dir)
				if err != nil {
					return nil, err
				}
				continue
			}
			if head > tail+min {
				rh.write(buf[offset])
			} else if tail+min < head+loaded-offset {
				// jump to the mn
				offset += tail + min - head
				head = tail + min
				if offset < winSZ {
					rh = newRollingHash(append(prev[offset:], buf[:offset]...))
				} else {
					rh = newRollingHash(buf[offset-winSZ : offset])
				}
			} else {
				// the threshold out of the range
				writeSegment(buf[offset:], chunker, hasher)
				head += loaded - offset
				// TODO: what if loaded < winSZ?
				copy(prev, buf[loaded-winSZ:loaded])
				break
			}

			if rh.sum64()%(1<<6) == 0 {
				if offset > marker {
					writeSegment(buf[marker:offset], chunker, hasher)
				}
				name, _, err = closeChunk(chunker, hasher)
				if err != nil {
					return nil, err
				}
				chunks = append(chunks, name)

				marker = offset
				tail = head
				hasher, chunker, err = reset(dir)
				if err != nil {
					return nil, err
				}
				continue
			}
			head++
			offset++
		}
		// finished processing this chunk
		if head > tail+min && head <= tail+max {
			writeSegment(buf[marker:loaded], chunker, hasher)
		}
		if head == tail+max {
			// write out chunk and start over
			name, _, err = closeChunk(chunker, hasher)
			if err != nil {
				return nil, err
			}
			chunks = append(chunks, name)

			tail = head
			hasher, chunker, err = reset(dir)
			if err != nil {
				return nil, err
			}
		}
	}
	if head > tail {
		// this is the last chunk
		name, _, err = closeChunk(chunker, hasher)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, name)
	}
	return chunks, nil
}
