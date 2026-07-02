import { useState } from 'react'
import { Microscope, Shield, Loader2, CheckCircle, XCircle, Copy } from 'lucide-react'
import { getApiBase, setAuthToken } from '@/api'
import { copyToClipboard } from '@/lib/clipboard'

interface LoginScreenProps {
  onAuthenticated: () => void
}

export function LoginScreen({ onAuthenticated }: LoginScreenProps) {
  const [step, setStep] = useState<'idle' | 'challenged' | 'verifying' | 'error'>('idle')
  const [filePath, setFilePath] = useState('')
  const [code, setCode] = useState('')
  const [error, setError] = useState('')
  const [requesting, setRequesting] = useState(false)
  const [copied, setCopied] = useState(false)

  const handleRequestChallenge = async () => {
    setRequesting(true)
    setError('')
    try {
      const res = await fetch(`${getApiBase()}/auth/challenge`, { method: 'POST' })
      if (res.ok) {
        const data = await res.json()
        setFilePath(data.file_path)
        setStep('challenged')
      } else {
        const err = await res.json().catch(() => ({}))
        setError(err.detail || 'Failed to create challenge. Is the network drive mounted on the server?')
        setStep('error')
      }
    } catch {
      setError('Cannot reach server.')
      setStep('error')
    } finally {
      setRequesting(false)
    }
  }

  const handleVerify = async () => {
    if (!code.trim()) return
    setStep('verifying')
    setError('')
    try {
      const res = await fetch(`${getApiBase()}/auth/verify`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ code: code.trim() }),
      })
      if (res.ok) {
        const data = await res.json()
        setAuthToken(data.token)
        onAuthenticated()
      } else {
        setError('Invalid or expired code. Try again.')
        setStep('challenged')
      }
    } catch {
      setError('Cannot reach server.')
      setStep('error')
    }
  }

  const handleCopyPath = () => {
    copyToClipboard(filePath).then((ok) => {
      if (!ok) return
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') handleVerify()
  }

  return (
    <div className="flex h-screen items-center justify-center" style={{ backgroundColor: '#111' }}>
      <div className="w-full max-w-md mx-4">
        {/* Brand */}
        <div className="text-center mb-10">
          <div className="inline-flex h-12 w-12 items-center justify-center rounded-sm bg-primary mb-4">
            <Microscope className="h-6 w-6 text-white" />
          </div>
          <h1 className="text-2xl font-semibold text-white tracking-tight">SlideCap</h1>
          <p className="text-[13px] text-neutral-500 mt-1">Pathology Slide Management</p>
        </div>

        {/* Card */}
        <div className="bg-neutral-900 border border-neutral-800 rounded-sm p-6">
          {(step === 'idle' || step === 'error') && (
            <div className="space-y-5">
              <div className="text-center">
                <Shield className="h-8 w-8 text-neutral-500 mx-auto mb-3" />
                <h2 className="text-[14px] font-medium text-white mb-1">Verify Network Access</h2>
                <p className="text-[12px] text-neutral-500 leading-relaxed">
                  To use SlideCap, you must have access to the lab network drive.
                  Click below to generate a verification code on the drive.
                </p>
              </div>

              {error && (
                <div className="flex items-start gap-2 p-2.5 border border-red-900/50 bg-red-950/30 text-[12px] text-red-400 rounded-sm">
                  <XCircle className="h-3.5 w-3.5 shrink-0 mt-0.5" />
                  <span>{error}</span>
                </div>
              )}

              <button
                onClick={handleRequestChallenge}
                disabled={requesting}
                className="w-full h-11 flex items-center justify-center gap-2 bg-primary hover:bg-primary/90 text-white text-[13px] font-medium rounded-sm transition-colors disabled:opacity-50"
              >
                {requesting ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Shield className="h-4 w-4" />
                )}
                {requesting ? 'Generating code...' : 'Generate Verification Code'}
              </button>
            </div>
          )}

          {(step === 'challenged' || step === 'verifying') && (
            <div className="space-y-5">
              <div>
                <h2 className="text-[14px] font-medium text-white mb-1">Enter Verification Code</h2>
                <p className="text-[12px] text-neutral-500 leading-relaxed">
                  A 6-digit code has been written to the network drive. Open the file below
                  and enter the code.
                </p>
              </div>

              {/* File path */}
              <div className="space-y-1.5">
                <label className="text-[11px] font-medium text-neutral-400 uppercase tracking-wider">File Location</label>
                <div className="flex items-center gap-1.5">
                  <div className="flex-1 p-2 bg-neutral-950 border border-neutral-800 rounded-sm text-[11px] font-mono text-neutral-300 break-all select-all">
                    {filePath}
                  </div>
                  <button
                    onClick={handleCopyPath}
                    className="shrink-0 p-2 text-neutral-500 hover:text-white transition-colors"
                    title="Copy path"
                  >
                    {copied ? <CheckCircle className="h-3.5 w-3.5 text-green-400" /> : <Copy className="h-3.5 w-3.5" />}
                  </button>
                </div>
              </div>

              {/* Code input */}
              <div className="space-y-1.5">
                <label className="text-[11px] font-medium text-neutral-400 uppercase tracking-wider">Verification Code</label>
                <input
                  type="text"
                  inputMode="numeric"
                  maxLength={6}
                  value={code}
                  onChange={e => setCode(e.target.value.replace(/\D/g, ''))}
                  onKeyDown={handleKeyDown}
                  placeholder="000000"
                  autoFocus
                  className="w-full h-12 text-center text-2xl font-mono tracking-[0.5em] bg-neutral-950 border border-neutral-700 focus:border-primary text-white rounded-sm outline-none transition-colors"
                />
              </div>

              {error && (
                <div className="flex items-start gap-2 p-2.5 border border-red-900/50 bg-red-950/30 text-[12px] text-red-400 rounded-sm">
                  <XCircle className="h-3.5 w-3.5 shrink-0 mt-0.5" />
                  <span>{error}</span>
                </div>
              )}

              <div className="flex gap-2">
                <button
                  onClick={() => { setStep('idle'); setCode(''); setError('') }}
                  className="flex-1 h-10 text-[13px] text-neutral-400 hover:text-white border border-neutral-700 hover:border-neutral-600 rounded-sm transition-colors"
                >
                  Back
                </button>
                <button
                  onClick={handleVerify}
                  disabled={code.length < 6 || step === 'verifying'}
                  className="flex-1 h-10 flex items-center justify-center gap-2 bg-primary hover:bg-primary/90 text-white text-[13px] font-medium rounded-sm transition-colors disabled:opacity-50"
                >
                  {step === 'verifying' ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <CheckCircle className="h-4 w-4" />
                  )}
                  {step === 'verifying' ? 'Verifying...' : 'Verify'}
                </button>
              </div>

              <p className="text-[11px] text-neutral-600 text-center">
                Code expires in 5 minutes
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
